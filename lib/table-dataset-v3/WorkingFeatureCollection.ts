import type z from 'zod';
import type { GeometryWithCrs } from '../utils/features/index.ts';
import {
  diffFeatureCollections,
  hasFeatureId,
  isGeoJsonFeature,
  type FeatureWithId,
} from '../utils/features/index.ts';
import { Emitter } from '../utils/index.ts';
import { checkFeatureCompliance } from './checkFeatureCompliance.ts';
import type { KartDiff } from './diffs.js';
import type { schemaEntrySchema } from './schemas/table-dataset-v3.ts';
import { makeSerializeable, parse, stringify } from './serializer.ts';

type FeatureCollection = { type: 'FeatureCollection'; features: FeatureWithId<GeometryWithCrs>[] };

export class WorkingFeatureCollection extends Emitter<{
  'feature:added': { featureId: string | number; feature: GeoJSON.Feature };
  'feature:deleted': { featureId: string | number };
  'feature:updated': { featureId: string | number; changes: Partial<GeoJSON.Feature> };
  feature: { type: 'added' | 'deleted' | 'updated'; partialFeature?: Partial<GeoJSON.Feature> };
}> {
  #initialFeatureCollection: FeatureCollection;
  #featureCollection: FeatureCollection;

  geometryType: Omit<'GeometryCollection', GeoJSON.Geometry['type']> | undefined;

  #schema: z.infer<typeof schemaEntrySchema>[];
  #datasetId: string;
  #primaryKeys: string[];
  #primaryGeometryKey: string;

  constructor(
    datasetId: string,
    featureCollection: FeatureCollection,
    schema: z.infer<typeof schemaEntrySchema>[]
  ) {
    super();

    this.#datasetId = datasetId;

    // see https://www.npmjs.com/package/devalue#:~:text=Other%20security%20considerations
    this.#initialFeatureCollection = parse(stringify(featureCollection));
    this.#featureCollection = parse(stringify(featureCollection));

    // ensure that all features have the same geometry type
    const geometryTypes = new Set(
      this.#featureCollection.features
        .map((f) => f.geometry?.type)
        .filter((t): t is GeoJSON.Geometry['type'] => !!t)
    );
    if (geometryTypes.size > 1) {
      throw new Error(`Feature collection for ${datasetId} has multiple geometry types.`);
    }
    if (geometryTypes.has('GeometryCollection')) {
      throw new Error(
        `GeometryCollection type is not supported in WorkingFeatureCollection (in ${datasetId}).`
      );
    }
    this.geometryType = [...geometryTypes][0];

    // we need the schema because we will validate changes to ensure they fit the schema
    this.#schema = schema;
    this.#primaryKeys = schema
      .filter(({ primaryKeyIndex }) => typeof primaryKeyIndex === 'number' && primaryKeyIndex >= 0)
      .map(({ name }) => name);
    const geometryColumns = schema.filter(({ dataType }) => dataType === 'geometry').map(({ name }) => name);
    this.#primaryGeometryKey = geometryColumns.includes('geom')
      ? 'geometry'
      : geometryColumns.includes('geom')
        ? 'geom'
        : geometryColumns[0];
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
  get featureCollection() {
    const schema = this.#schema;
    const classInstance = this;

    return makeSerializeable({
      type: 'FeatureCollection',
      features: new Proxy(this.#featureCollection.features, {
        // Validate features being set against the schema.
        set: (target, prop, value) => {
          const index = Number(prop);
          if (isNaN(index)) {
            throw new TypeError('You can only set features by numeric index.');
          }

          const oldFeature = target[index];
          const feature = value as FeatureWithId;
          const isChanged = stringify(oldFeature) !== stringify(feature);
          if (!isChanged) {
            return true;
          }

          const { valid, errors } = checkFeatureCompliance(feature, schema);
          if (!valid) {
            throw new TypeError(
              `Feature that was attempted to be set at index ${index} does not comply with schema: ${errors.join('; ')}`
            );
          }

          if (oldFeature) {
            super.emit('feature:deleted', { featureId: oldFeature.id });
            super.emit('feature', { type: 'deleted', partialFeature: { id: oldFeature.id } });
          }
          super.emit('feature:added', { featureId: feature.id!, feature });
          super.emit('feature', { type: 'added', partialFeature: feature });
          target[index] = feature;

          return true;
        },

        // Wrap features in a proxy that validates modifications.
        get: (target, prop, receiver) => {
          const index = Number(prop);
          if (isNaN(index)) {
            // built-in methods such as length, push, pop, etc.
            return Reflect.get(target, prop, receiver);
          }

          const maybeFeature = Reflect.get(target, prop, receiver);
          const feature = isGeoJsonFeature(maybeFeature) && hasFeatureId(maybeFeature) ? maybeFeature : null;
          if (!feature) {
            throw new TypeError(`Feature at index ${index} is not a valid GeoJSON Feature.`);
          }

          /**
           * Creates a proxy for a feature that enforces schema compliance on modifications.
           *
           * @remarks
           * This function wraps the feature and its nested objects in proxies
           * that validate any modifications against the schema. If a modification
           * causes the feature to become non-compliant, the change is reverted
           * and a TypeError is thrown.
           *
           * @param featureToCheck - The feature for which compliance is to be checked.
           * @param objToProxy - The current object to wrap in a proxy.
           * @param cache - A WeakMap to cache already proxied objects.
           */
          function createFeatureProxy(
            featureToCheck: FeatureWithId,
            objToProxy: object = featureToCheck,
            cache = new WeakMap<object, any>()
          ) {
            if (cache.has(objToProxy)) {
              return cache.get(objToProxy);
            }

            const handler: ProxyHandler<object> = {
              /**
               * Gets a property value from an object within the feature.
               *
               * @remarks
               * Nested objects are also wrapped in proxies to enforce
               * modification restrictions at all levels.
               */
              get(target, prop, receiver) {
                const val = Reflect.get(target, prop, receiver);
                if (val && typeof val === 'object') {
                  return createFeatureProxy(featureToCheck, val, cache);
                }
                return val;
              },

              /**
               * Sets a property value on an object within the feature.
               *
               * @remarks
               * If the modification causes the feature to become non-compliant
               * with the schema, the change is reverted and a TypeError is thrown.
               */
              set(target, prop, value, receiver) {
                const oldValue = Reflect.get(target, prop, receiver);
                const oldFeature = parse(stringify(featureToCheck));
                const setResult = Reflect.set(target, prop, value, receiver);

                const isChanged = stringify(oldValue) !== stringify(value);
                if (!isChanged) {
                  return setResult;
                }

                const { valid, errors } = checkFeatureCompliance(featureToCheck, schema);
                if (!valid) {
                  // revert the change
                  Reflect.set(target, prop, oldValue, receiver);
                  throw new TypeError(
                    `Modification to feature property "${String(prop)}" does not comply with schema: ${errors.join('; ')}`
                  );
                }

                if (oldValue && oldFeature && isGeoJsonFeature(oldFeature) && hasFeatureId(oldFeature)) {
                  classInstance.emit('feature:deleted', { featureId: oldFeature.id });
                  classInstance.emit('feature', { type: 'deleted', partialFeature: { id: oldFeature.id } });
                }
                classInstance.emit('feature:added', { featureId: featureToCheck.id, feature: featureToCheck });
                classInstance.emit('feature', { type: 'added', partialFeature: featureToCheck });

                return setResult;
              },

              /**
               * Deletes a property from an object within the feature.
               *
               * @remarks
               * If the deletion causes the feature to become non-compliant
               * with the schema, the deletion is reverted and a TypeError is thrown.
               */
              deleteProperty(target, prop) {
                const oldValue = Reflect.get(target, prop);
                const oldFeature = parse(stringify(featureToCheck));
                const deleteResult = Reflect.deleteProperty(target, prop);

                const { valid, errors } = checkFeatureCompliance(featureToCheck, schema);
                if (!valid) {
                  // revert the deletion
                  Reflect.set(target, prop, oldValue);
                  throw new TypeError(
                    `Deletion of feature property "${String(prop)}" does not comply with schema: ${errors.join('; ')}`
                  );
                }

                if (oldValue && oldFeature && isGeoJsonFeature(oldFeature) && hasFeatureId(oldFeature)) {
                  classInstance.emit('feature:deleted', { featureId: oldFeature.id });
                  classInstance.emit('feature', { type: 'deleted', partialFeature: { id: oldFeature.id } });
                }

                return deleteResult;
              },
            };

            const proxy = new Proxy(objToProxy, handler);
            cache.set(objToProxy, proxy);
            return proxy;
          }

          return createFeatureProxy(feature);
        },
      }),
    });
  }

  /**
   * Whether the feature collection has been modified since creation.
   */
  get isDirty() {
    return stringify(this.#initialFeatureCollection) !== stringify(this.#featureCollection);
  }

  /**
   * Gets a feature by its ID.
   */
  get(featureId: string | number) {
    return this.#featureCollection.features.find((f) => f.id === featureId);
  }

  /**
   * Gets the index of a feature by its ID.
   */
  getIndex(featureId: string | number) {
    return this.#featureCollection.features.findIndex((f) => f.id === featureId);
  }

  /**
   * Sets the properties of a feature by its ID.
   *
   * @remarks
   * By default, this method merges the provided properties with the existing properties.
   * To replace the properties entirely, set `merge` to `false`.
   *
   * If a property value is `undefined`, it will be deleted from the feature's properties.
   *
   * If not changes are detected, the feature is not modified.
   *
   * @param featureId - The ID of the feature to update.
   * @param properties - The properties to set on the feature.
   * @param merge - Whether to merge the new properties with the existing ones (default: true).
   */
  updateProperties(featureId: string | number, properties: Record<string, unknown>, merge = true) {
    const index = this.getIndex(featureId);
    if (index === -1) {
      throw new Error(`Feature with ID "${featureId}" not found.`);
    }

    let newProperties: Record<string, unknown> = {};
    if (merge) {
      newProperties = {
        ...this.#featureCollection.features[index].properties,
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

    const { valid, errors } = checkFeatureCompliance(
      {
        type: 'Feature',
        id: featureId,
        geometry: this.#featureCollection.features[index].geometry,
        properties: newProperties,
      },
      this.#schema
    );
    if (!valid) {
      throw new Error(
        `Updated geometry for feature ID "${featureId}" does not comply with schema: ${errors.join('; ')}`
      );
    }

    // only update if there are changes
    const hasChanges =
      stringify(this.#featureCollection.features[index].properties) !== stringify(newProperties);
    if (hasChanges) {
      this.#featureCollection.features[index].properties = newProperties;
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
  updateGeometry(featureId: string | number, geometry: GeoJSON.Geometry) {
    const index = this.#featureCollection.features.findIndex((f) => f.id === featureId);
    if (index === -1) {
      throw new Error(`Feature with ID "${featureId}" not found.`);
    }

    if (this.#featureCollection.features[index].geometry.type !== geometry.type) {
      throw new Error(
        `Cannot change geometry type from "${this.#featureCollection.features[index].geometry.type}" to "${geometry.type}".`
      );
    }

    this.#featureCollection.features[index].geometry = geometry;
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
  add(feature: FeatureWithId) {
    if (feature.id === undefined || feature.id === null) {
      throw new Error('Feature must have an ID to be added.');
    }

    if (this.get(feature.id) !== undefined) {
      throw new Error(`Feature with ID "${feature.id}" already exists.`);
    }

    if (feature.geometry.type !== this.geometryType) {
      throw new Error(
        `Cannot add feature with geometry type "${feature.geometry.type}" to collection with geometry type "${this.geometryType}".`
      );
    }

    const { valid, errors } = checkFeatureCompliance(feature, this.#schema);
    if (!valid) {
      throw new Error(
        `Feature to add with ID "${feature.id}" does not comply with schema: ${errors.join('; ')}`
      );
    }

    this.#featureCollection.features.push(feature);
    super.emit('feature:added', { featureId: feature.id, feature });
    super.emit('feature', { type: 'added', partialFeature: feature });
  }

  /**
   * Removes a feature from the collection by its ID.
   */
  delete(featureId: string | number) {
    const index = this.getIndex(featureId);
    if (index === -1) {
      throw new Error(`Feature with ID "${featureId}" not found.`);
    }
    this.#featureCollection.features.splice(index, 1);
    super.emit('feature:deleted', { featureId });
    super.emit('feature', { type: 'deleted', partialFeature: { id: featureId } });
  }

  /**
   * Gets a diff of the changes made to the feature collection since creation.
   */
  get diff() {
    if (!this.isDirty) {
      return {
        'kart.diff/v1+hexwkb': makeSerializeable<KartDiff.HexWkB.v1.Diff>({
          [this.#datasetId]: {},
        }),
      };
    }

    const featureCollectionDiff = diffFeatureCollections(
      this.#featureCollection,
      this.#initialFeatureCollection
    );

    // We will resolve the feature collection diff into
    // insertions, deletions, or modifications that
    // include all primary key values, which is required
    // for the Kart diff format version 1.
    let changes: (
      | { type: 'insert' | 'delete'; data: Record<string, unknown> }
      | { type: 'update'; data: { oldValues: Record<string, unknown>; newValues: Record<string, unknown> } }
    )[] = [];

    featureCollectionDiff.deleted.forEach((featureId) => {
      const originalFeature = this.#initialFeatureCollection.features.find(
        (feature) => feature.id === featureId
      );
      if (!originalFeature) {
        throw new Error(`Feature with ID "${featureId}" not found in initial feature collection.`);
      }

      const originalPrimaryKeyValues: Record<string, unknown> = {};
      this.#primaryKeys.map((primaryKey, index) => {
        if (index === 0) {
          originalPrimaryKeyValues[primaryKey] = featureId;
        } else {
          // All primary keys must be present in the diff.
          // Since serializing undefined to JSON removes the key,
          // we must use null for empty or missing primary key values.
          originalPrimaryKeyValues[primaryKey] = originalFeature.properties?.[primaryKey] ?? null;
        }
      });

      changes.push({ type: 'delete', data: originalPrimaryKeyValues });
    });

    featureCollectionDiff.inserted.forEach((feature) => {
      const data: Record<string, unknown> = {};
      data[this.#primaryKeys[0]] = feature.id;
      if (feature.properties) {
        Object.assign(data, feature.properties);
      }
      data[this.#primaryGeometryKey] = feature.geometry;
      changes.push({ type: 'insert', data });
    });

    featureCollectionDiff.modified.forEach((modification) => {
      const originalFeature = this.#initialFeatureCollection.features.find(
        (feature) => feature.id === modification.id
      );
      if (!originalFeature) {
        throw new Error(`Feature with ID "${modification.id}" not found in initial feature collection.`);
      }

      const originalPrimaryKeyValues: Record<string, unknown> = {};
      this.#primaryKeys.map((primaryKey, index) => {
        if (index === 0) {
          originalPrimaryKeyValues[primaryKey] = modification.id;
        } else {
          // Since serializing undefined to JSON removes the key,
          // we must use null for empty or missing primary key values.
          originalPrimaryKeyValues[primaryKey] = originalFeature.properties?.[primaryKey] ?? null;
        }
      });

      const newPrimaryKeyValues: Record<string, unknown> = {};
      this.#primaryKeys.map((primaryKey, index) => {
        if (index === 0) {
          newPrimaryKeyValues[primaryKey] = modification.id;
        } else {
          // Since serializing undefined to JSON removes the key,
          // we must use null for empty or missing primary key values.
          newPrimaryKeyValues[primaryKey] =
            modification.properties?.[primaryKey] ?? originalFeature.properties?.[primaryKey] ?? null;
        }
      });

      // if any primary key value has changed, we need to treat this as a delete and insert
      const primaryKeyHasChanged = this.#primaryKeys.some((primaryKey) => {
        return stringify(originalPrimaryKeyValues[primaryKey]) !== stringify(newPrimaryKeyValues[primaryKey]);
      });

      if (primaryKeyHasChanged) {
        // create a full data object for the insertion
        // that includes the modifications on top of the original properties
        const data: Record<string, unknown> = { ...newPrimaryKeyValues };
        if (originalFeature.properties) {
          Object.assign(data, originalFeature.properties);
        }
        if (modification.properties) {
          Object.assign(data, modification.properties);
        }
        data[this.#primaryGeometryKey] = modification.geometry ?? originalFeature.geometry;

        changes.push({ type: 'delete', data: originalPrimaryKeyValues });
        changes.push({ type: 'insert', data });
        return;
      }

      // for updates, we only include changed properties and geometry
      const data = {
        oldValues: { ...originalPrimaryKeyValues },
        newValues: { ...originalPrimaryKeyValues },
      };
      for (const [key, value] of Object.entries(modification.properties || {})) {
        data.newValues[key] = value;
        data.oldValues[key] = originalFeature.properties?.[key] ?? null;
      }
      const hasGeometryChanged = modification.geometry !== undefined;
      if (hasGeometryChanged) {
        data.newValues[this.#primaryGeometryKey] = modification.geometry;
        data.oldValues[this.#primaryGeometryKey] = originalFeature.geometry;
      }
      changes.push({ type: 'update', data });
    });

    const diff: KartDiff.HexWkB.v1.Diff = {
      [this.#datasetId]: {
        feature: changes
          .map((change) => {
            if (change.type === 'insert') {
              return {
                '++': change.data,
              } satisfies KartDiff.HexWkB.v1.Insert;
            }

            if (change.type === 'delete') {
              return {
                '--': change.data,
              } satisfies KartDiff.HexWkB.v1.Delete;
            }

            if (change.type === 'update') {
              return {
                // '-': change.data.oldValues, // not allowed when use reprojected patches
                '+': change.data.newValues,
              } satisfies KartDiff.HexWkB.v1.Update;
            }
          })
          .filter((x) => !!x),
      },
    };

    return {
      'kart.patch/v1': {
        base: null,
        crs: 'EPSG:4326',
      },
      'kart.diff/v1+hexwkb': makeSerializeable(diff),
    };
  }
}
