import type { GeometryWithCrs, KartEnabledFeature, KartFeatureCollection } from '../utils/features/index.ts';
import { isKartEnabledFeature } from '../utils/features/index.ts';
import { deepFreeze, Emitter } from '../utils/index.ts';
import type { CRSs } from './CRS.ts';
import type { KartDiff } from './diffs.js';
import { AggregateValidationError, Feature } from './Feature.ts';
import type { Schema } from './Schema.ts';
import { makeSerializable, parse, stringify } from './serializer.ts';
import type { TableDatasetV3 } from './TableDatasetV3.ts';

type KartFeature = KartFeatureCollection['features'][number];

/**
 * The working copy tracks changes to a dataset's features.
 *
 * @remarks
 * The working copy never modifies the original features directly.
 * Instead, it tracks changes as a set of diffs that can be applied
 * to the original features to produce the current state of the
 * working copy.
 *
 * The working copy does not read the entire dataset into memory.
 * Instead, it reads features on demand when they are accessed or modified.
 *
 * To get the current state of the working copy as a GeoJSON FeatureCollection,
 * use the `toGeoJSON` method, which applies the tracked changes to the original
 * features and returns a new GeoJSON FeatureCollection representing the current
 * state of the working copy. If the dataset is large, reading it to GeoJSON
 * the first time may be slow. Subsequent calls to `toGeoJSON` will be faster
 * because the dataset caches its geoJSON in memory once the dataset has been read once.
 *
 * To get a diff of the changes made to the working copy, use the `diff` getter,
 * which returns a diff in the `kart.diff/v1+hexwkb` format.
 *
 * To access the tracked changes directly, use the `changes` generator, which
 * yields each change as a tuple of `[eid, change]`, where `eid` is the
 * encoded ID of the feature and `change` is a `TrackedChange` object.
 */
export class WorkingFeatureCollection extends Emitter<{
  'feature:added': { featureId: string | number; feature: GeoJSON.Feature };
  'feature:deleted': { featureId: string | number };
  'feature:updated': { featureId: string | number; changes: Partial<GeoJSON.Feature> };
  feature: { type: 'added' | 'deleted' | 'updated'; partialFeature?: Partial<GeoJSON.Feature> };
}> {
  geometryType: Omit<'GeometryCollection', GeoJSON.Geometry['type']> | undefined;

  private dataset: TableDatasetV3;
  private datasetId: string;
  private primaryKeys: string[];
  private primaryGeometryKey: string;

  private trackedChanges: TrackedChanges;

  constructor(dataset: TableDatasetV3) {
    super();

    this.dataset = dataset;
    this.datasetId = dataset.id;
    this.trackedChanges = new TrackedChanges();

    // ensure that all features have the same geometry type
    // const geometryTypes = new Set(
    //   this.#featureCollection.features
    //     .map((f) => f.geometry?.type)
    //     .filter((t): t is GeoJSON.Geometry['type'] => !!t)
    // );
    // if (geometryTypes.size > 1) {
    //   throw new Error(
    //     `Feature collection for ${datasetId} has multiple geometry types. Found: ${[...geometryTypes].join(', ')}. All features in a WorkingFeatureCollection must have the same geometry type.`
    //   );
    // }
    // if (geometryTypes.has('GeometryCollection')) {
    //   throw new Error(
    //     `GeometryCollection type is not supported in WorkingFeatureCollection (in ${datasetId}).`
    //   );
    // }
    // this.geometryType = [...geometryTypes][0];

    // we will use the primary keys to ensure that the diffs we generate
    // always include the primary keys first
    this.primaryKeys = dataset.schema
      .filter(({ primaryKeyIndex }) => typeof primaryKeyIndex === 'number' && primaryKeyIndex >= 0)
      .map(({ name }) => name);

    const geometryColumns = dataset.schema
      .filter(({ dataType }) => dataType === 'geometry')
      .map(({ name }) => name);
    if (geometryColumns.length === 0) {
      throw new Error(`Schema for dataset ${dataset.id} does not have a geometry column.`);
    }
    this.primaryGeometryKey = geometryColumns.includes('geom')
      ? 'geometry'
      : geometryColumns.includes('geom')
        ? 'geom'
        : geometryColumns[0]!;
  }

  /**
   * The working copy of the feature collection.
   *
   * @remarks
   * This getter returns the current state of the feature collection.
   * The features array is a proxy to the internal feature collection,
   * and modifications made directly to the proxy will modify the
   * internal state. The proxy enables validation of changes, ensuring
   * that every modification adheres to the defined schema.
   *
   * The prefered way to access and modify features is through the
   * `get`, `updateProperties`, `updateGeometry`, `add`, and `delete` methods.
   */
  async toGeoJSON() {
    const datasetGeoJSON: KartFeatureCollection = parse(stringify(await this.dataset.toGeoJSON()));

    for (const [eid, change] of this.trackedChanges) {
      const featureIndex = datasetGeoJSON.features.findIndex((f) => f._kart.eid === eid);

      if (change.type === 'delete') {
        if (featureIndex === -1) {
          throw new Error(
            `Inconsistent state: attempted to delete feature with eid "${eid}" that does not exist in the dataset GeoJSON.`
          );
        }

        datasetGeoJSON.features.splice(featureIndex, 1);
      }

      if (change.type === 'insert') {
        if (featureIndex !== -1) {
          throw new Error(
            `Inconsistent state: attempted to insert feature with eid "${eid}" that already exists in the dataset GeoJSON.`
          );
        }

        datasetGeoJSON.features.push(change.feature);
      }

      if (change.type === 'update') {
        if (featureIndex === -1) {
          throw new Error(
            `Inconsistent state: attempted to update feature with eid "${eid}" that does not exist in the dataset GeoJSON.`
          );
        }

        const existingFeature = datasetGeoJSON.features[featureIndex];
        if (!existingFeature) {
          throw new Error(
            `Inconsistent state: attempted to update feature with eid "${eid}" but could not find the existing feature in the dataset GeoJSON.`
          );
        }

        if ('geometry' in change) {
          existingFeature.geometry = change.geometry;
        }

        if ('properties' in change) {
          change.properties ??= {}; // ensure properties object exists
          for (const [key, value] of Object.entries(change.properties)) {
            if (value === undefined) {
              delete existingFeature.properties?.[key];
            } else {
              existingFeature.properties![key] = value;
            }
          }
        }
      }
    }

    return deepFreeze({
      type: 'FeatureCollection',
      features: datasetGeoJSON.features,
    } as KartFeatureCollection);
  }

  /**
   * Whether the feature collection has been modified since creation.
   */
  get isDirty() {
    return this.trackedChanges.size > 0;
  }

  has(featureId: string): boolean {
    return this.get(featureId) !== undefined;
  }

  /**
   * Gets a feature by its ID.
   */
  get(featureId: string): KartFeature | undefined {
    const originalFeature = this.dataset.get(featureId)?.toGeoJSON() ?? undefined;

    if (this.trackedChanges.has(featureId)) {
      const change = this.trackedChanges.get(featureId)!;

      if (change.type === 'delete') {
        return undefined;
      }

      if (
        change.type === 'insert' &&
        isKartEnabledFeature(change.feature) &&
        checkFeatureCompliance(change.feature, this.dataset.schema, this.dataset.crss).valid
      ) {
        return change.feature;
      }

      if (change.type === 'update' && originalFeature) {
        const geometry = 'geometry' in change ? change.geometry : originalFeature.geometry;
        const properties =
          'properties' in change
            ? {
                ...originalFeature.properties,
                ...change.properties,
              }
            : originalFeature.properties;

        return {
          ...originalFeature,
          geometry,
          properties,
        };
      }
    }

    if (
      originalFeature &&
      checkFeatureCompliance(originalFeature, this.dataset.schema, this.dataset.crss).valid
    ) {
      return originalFeature;
    }
  }

  /**
   * Sets the properties of a feature by its ID.
   *
   * @remarks
   * By default, this method merges the provided properties with the existing properties
   * that have already been tracked in a previous change.
   * To replace the tracked change properties entirely, set `merge` to `false`.
   *
   * If a property value is `undefined`, it will be deleted from the feature's properties.
   *
   * If not changes are detected, the feature is not modified.
   *
   * @param featureId - The ID of the feature to update.
   * @param properties - The properties to set on the feature.
   * @param merge - Whether to merge the updated properties with the existing tracked update (default: true).
   */
  updateProperties(featureId: string, properties: Record<string, unknown>, merge = true) {
    if (!this.has(featureId)) {
      throw new Error(`Feature with ID "${featureId}" not found.`);
    }

    const currentFeature = this.get(featureId)!;

    let newProperties: Record<string, unknown> = {};
    if (merge) {
      newProperties = {
        ...currentFeature.properties,
        ...properties,
      };
    } else {
      newProperties = properties;
    }

    // delete undefined values
    for (const key of Object.keys(newProperties)) {
      if (newProperties[key] === undefined) {
        delete newProperties[key];
      }
    }

    // delete values that are the same as the original feature to avoid unnecessary updates
    const originalFeature = this.dataset.get(featureId)?.toGeoJSON();
    if (!originalFeature) {
      throw new Error(
        `Inconsistent state: attempted to update properties for feature with ID "${featureId}" but could not find the original feature in the dataset.`
      );
    }
    for (const key of Object.keys(newProperties)) {
      if (originalFeature.properties && key in originalFeature.properties) {
        if (stringify(newProperties[key]) === stringify(originalFeature.properties?.[key])) {
          delete newProperties[key];
        }
      }
    }

    const { valid, errors } = checkFeatureCompliance(
      {
        ...currentFeature,
        properties: newProperties,
      },
      this.dataset.schema,
      this.dataset.crss
    );
    if (!valid) {
      throw new Error(
        `Attempted update for feature with ID "${featureId}" does not comply with the schema: ${errors.join('; ')}`
      );
    }

    // only update if there are changes
    const hasChanges =
      Object.keys(newProperties).length !== Object.keys(currentFeature.properties || {}).length ||
      stringify(currentFeature.properties) !== stringify(newProperties);
    if (hasChanges) {
      this.trackedChanges.setProperties(featureId, { properties: newProperties });
      super.emit('feature:updated', { featureId, changes: { properties: newProperties } });
      super.emit('feature', { type: 'updated', partialFeature: { id: featureId, properties: newProperties } });
    }
  }

  /**
   * Replaces the geometry of a feature by its ID.
   *
   * @remarks
   * Even if there are no changes detected, the geometry is replaced.
   *
   * Geometry type must not be changed.
   *
   * @param featureId - The ID of the feature to update.
   * @param geometry - The new geometry for the feature.
   */
  updateGeometry(featureId: string, geometry: GeoJSON.Geometry) {
    if (!this.has(featureId)) {
      throw new Error(`Feature with ID "${featureId}" not found.`);
    }

    if (geometry.type !== this.geometryType) {
      throw new Error(
        `Cannot change geometry of type "${geometry.type}" for feature with ID "${featureId}" in a collection with geometry type "${this.geometryType}".`
      );
    }

    this.trackedChanges.setGeometry(featureId, { geometry });
    super.emit('feature:updated', { featureId, changes: { geometry } });
    super.emit('feature', { type: 'updated', partialFeature: { id: featureId, geometry } });
  }

  /**
   * Adds a new feature to the collection.
   *
   * @remarks
   * If the feature ID already exists, an error is thrown.
   *
   * @param feature - The feature to add.
   */
  add(feature: KartFeature) {
    if (feature.id === undefined || feature.id === null) {
      throw new Error('Feature must have an ID to be added.');
    }

    if (this.get(feature.id) !== undefined) {
      throw new Error(`Feature with ID "${feature.id}" already exists.`);
    }

    if (feature.geometry.type !== this.geometryType) {
      throw new Error(
        `Cannot add feature with ID "${feature.id}" and geometry type "${feature.geometry.type}" to a collection with geometry type "${this.geometryType}".`
      );
    }

    const { valid, errors } = checkFeatureCompliance(feature, this.dataset.schema, this.dataset.crss);
    if (!valid) {
      throw new Error(
        `Feature to add with ID "${feature.id}" does not comply with the schema: ${errors.join('; ')}`
      );
    }

    this.trackedChanges.setInsert(feature.id.toString(), { feature });
    super.emit('feature:added', { featureId: feature.id, feature });
    super.emit('feature', { type: 'added', partialFeature: feature });
  }

  /**
   * Removes a feature from the collection by its ID.
   */
  delete(featureId: string) {
    if (!this.has(featureId)) {
      throw new Error(`Feature with ID "${featureId}" not found.`);
    }

    const originalFeature = this.dataset.get(featureId)?.toGeoJSON() ?? undefined;
    const primaryKeys = Object.keys(originalFeature?._kart.ids || {}); // no primary keys === no original feature to delete

    this.trackedChanges.setDelete(featureId);
    super.emit('feature:deleted', { featureId });
    super.emit('feature', { type: 'deleted', partialFeature: { id: featureId } });
  }

  *changes() {
    for (const [featureId, change] of this.trackedChanges) {
      yield [featureId, change] as const;
    }
  }

  /**
   * Gets a diff of the changes made to the feature collection since creation.
   */
  get diff() {
    if (this.trackedChanges.size === 0) {
      return {
        'kart.diff/v1+hexwkb': makeSerializable<KartDiff.HexWkB.v1.Diff>({
          [this.datasetId]: {},
        }),
      };
    }

    const diff: KartDiff.HexWkB.v1.Diff = {
      [this.datasetId]: {
        feature: this.trackedChanges
          .map((change, eid) => {
            if (change.type === 'insert') {
              const data: Record<string, unknown> = {};

              // always include primary keys first
              for (const key of this.primaryKeys) {
                data[key] = change.feature._kart.ids[key];
              }
              data[this.primaryGeometryKey] = change.feature.geometry;

              for (const key of Object.keys(change.feature.properties || {})) {
                if (data[key] === undefined) {
                  data[key] = change.feature.properties![key];
                }
              }

              return {
                '++': data,
              } satisfies KartDiff.HexWkB.v1.Insert;
            }

            if (change.type === 'delete') {
              const originalFeature = this.dataset.get(eid);
              if (!originalFeature) {
                throw new Error(`Original feature with ID "${eid}" not found for update diff generation.`);
              }

              const data: Record<string, unknown> = {};
              for (const key of this.primaryKeys) {
                data[key] = originalFeature.ids.get(key) ?? null;
              }

              return {
                '--': data,
              } satisfies KartDiff.HexWkB.v1.Delete;
            }

            if (change.type === 'update') {
              const originalFeature = this.dataset.get(eid);
              if (!originalFeature) {
                throw new Error(`Original feature with ID "${eid}" not found for update diff generation.`);
              }

              const newValues: Record<string, unknown> = {};
              for (const key of this.primaryKeys) {
                newValues[key] = originalFeature.ids.get(key) ?? null;
              }

              const oldValues: Record<string, unknown> = parse(stringify(newValues));

              if ('geometry' in change) {
                newValues[this.primaryGeometryKey] = change.geometry;
                const oldValueResult = originalFeature.getValue(this.primaryGeometryKey);
                if (oldValueResult.ok) {
                  oldValues[this.primaryGeometryKey] = oldValueResult.data;
                } else {
                  throw new Error(
                    `Failed to get original geometry value for feature ID "${eid}" when generating update diff: ${oldValueResult.errors?.map((e) => e.message).join('; ')}`
                  );
                }
              }

              if ('properties' in change) {
                for (const [key, value] of Object.entries(change.properties || {})) {
                  // only add if it does not conflict with primary keys or geometry
                  if (newValues[key] === undefined) {
                    newValues[key] = value;
                  }
                  if (oldValues[key] === undefined) {
                    const oldValueResult = originalFeature.getValue(key);
                    if (oldValueResult.ok) {
                      oldValues[key] = oldValueResult.data;
                    } else {
                      throw new Error(
                        `Failed to get original property "${key}" value for feature ID "${eid}" when generating update diff: ${oldValueResult.errors?.map((e) => e.message).join('; ')}`
                      );
                    }
                  }
                }
              }

              return {
                // '-': change.data.oldValues, // not allowed when use reprojected patches
                '+': newValues,
              } satisfies KartDiff.HexWkB.v1.Update;
            }
          })
          .filter((x) => !!x),
      },
    };

    return {
      'kart.patch/v1': {
        base: null,
        crs: 'EPSG:4326', // we use reprojected geometries in the diff, so the CRS is always EPSG:4326
      },
      'kart.diff/v1+hexwkb': makeSerializable(diff),
    };
  }
}

type TrackedDelete = { type: 'delete' };
type TrackedInsert = { type: 'insert'; feature: KartFeature };
type TrackedPropertiesUpdate = { type: 'update'; properties: Partial<KartFeature['properties']> };
type TrackedGeometryUpdate = { type: 'update'; geometry: KartFeature['geometry'] };
type TrackedGeometryAndPropertiesUpdate = {
  type: 'update';
  properties: Partial<KartFeature['properties']>;
  geometry: KartFeature['geometry'];
};
export type TrackedChange =
  | TrackedDelete
  | TrackedInsert
  | TrackedPropertiesUpdate
  | TrackedGeometryUpdate
  | TrackedGeometryAndPropertiesUpdate;

class TrackedChanges implements Omit<
  Map<string, TrackedChange>,
  'set' | 'delete' | 'forEach' | 'entries' | 'values'
> {
  trackedChanges: Map<string, TrackedChange> = new Map();

  clear() {
    this.trackedChanges.clear();
  }

  /**
   * Removes a tracked change.
   *
   * To track a deletion, use `setDelete` instead.
   */
  private delete(key: string) {
    return this.trackedChanges.delete(key);
  }

  has(key: string) {
    return this.trackedChanges.has(key);
  }

  get(key: string) {
    return this.trackedChanges.get(key);
  }

  get size() {
    return this.trackedChanges.size;
  }

  keys() {
    return this.trackedChanges.keys();
  }

  [Symbol.iterator]() {
    return this.trackedChanges[Symbol.iterator]();
  }

  get [Symbol.toStringTag]() {
    return 'TrackedChanges';
  }

  /**
   * Track a deletion of a feature.
   */
  setDelete(key: string): this {
    if (!this.has(key)) {
      this.trackedChanges.set(key, { type: 'delete' });
      return this;
    }

    const current = this.get(key);
    if (current?.type === 'delete') {
      return this;
    }

    if (current?.type === 'insert') {
      // deleting a feature that already has an insert tracked
      // idicates that we can just remove the tracked change
      this.delete(key);
      return this;
    }

    this.trackedChanges.set(key, { type: 'delete' });
    return this;
  }

  /**
   * Track the insertion of a new feature.
   */
  setInsert(key: string, value: Omit<TrackedInsert, 'type'>): this {
    this.trackedChanges.set(key, { type: 'insert', ...value });
    return this;
  }

  /**
   * Register an update to the geometry of a feature.
   */
  setGeometry(key: string, value: Omit<TrackedGeometryUpdate, 'type'>): this {
    if (!this.has(key)) {
      this.trackedChanges.set(key, { type: 'update', ...value });
      return this;
    }

    const current = this.get(key)!;

    // merge with existing update
    if (current.type === 'update' && 'properties' in current) {
      this.trackedChanges.set(key, {
        type: 'update',
        properties: current.properties,
        geometry: value.geometry,
      });
      return this;
    }

    this.trackedChanges.set(key, { type: 'update', ...value });
    return this;
  }

  /**
   * Register an update to the properties of a feature.
   *
   * The properties MUST be all properties that are changed from
   * the original feature, not since the last update.
   *
   * DO NOT pass the full set of properties.
   */
  setProperties(key: string, value: Omit<TrackedPropertiesUpdate, 'type'>): this {
    if (!this.has(key)) {
      this.trackedChanges.set(key, { type: 'update', ...value });
      return this;
    }

    const current = this.get(key)!;

    // merge with existing update geometry
    if (current.type === 'update' && 'geometry' in current) {
      this.trackedChanges.set(key, {
        type: 'update',
        properties: value.properties,
        geometry: current.geometry,
      });
      return this;
    }

    this.trackedChanges.set(key, { type: 'update', ...value });
    return this;
  }

  map<U>(callback: (value: TrackedChange, key: string, map: Map<string, TrackedChange>) => U): U[] {
    const results: U[] = [];
    for (const [key, value] of this.trackedChanges) {
      results.push(callback(value, key, this.trackedChanges));
    }
    return results;
  }
}

function checkFeatureCompliance(feature: KartEnabledFeature<GeometryWithCrs>, schema: Schema, crss: CRSs) {
  try {
    Feature.fromGeoJSON(feature, schema, crss);
    return { valid: true, errors: [] };
  } catch (error) {
    if (error instanceof AggregateValidationError) {
      return {
        valid: false,
        errors: Array.from(error.errors).map(([key, error]) => {
          const newError = new Error(`Property "${key}": ${error.map((e) => e.message).join('; ')}`);
          newError.name = error[0]?.name || 'ValidationError';
          newError.stack = error[0]?.stack;
          newError.cause = error[0]?.cause;
          return newError;
        }),
      };
    }
    return { valid: false, errors: [error instanceof Error ? error : new Error(`${error}`)] };
  }
}
