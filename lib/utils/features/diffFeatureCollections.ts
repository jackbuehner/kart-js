import { Feature } from '../../table-dataset-v3/Feature.ts';
import serializer from '../../table-dataset-v3/serializer.ts';
import { hasFeatureId } from './hasFeatureId.ts';
import type { FeatureWithId, KartEnabledFeature, KartFeatureCollection } from './index.ts';

interface FeatureCollectionDiff {
  inserted: KartEnabledFeature[];
  deleted: string[];
  modified: {
    id: string;
    properties?: Record<string, unknown>;
    ids?: Record<string, unknown>;
    geometry?: GeoJSON.Geometry;
  }[];
}

/**
 * Calculates the difference between two GeoJSON FeatureCollections.
 *
 * @remarks
 * This function reports inserted, deleted, and modified features between the two collections
 * differently:
 * - For inserted features, the entire feature is reported.
 * - For deleted features, only the `id` of the feature is reported.
 * - For modified features, only modified properties or geometry are reported.
 *
 * The input feature collections MUST have `id` properties on all features.
 *
 * @param ours - The feature collection to compare.
 * @param reference - The feature collection to compare against.
 */
export function diffFeatureCollections(
  ours: KartFeatureCollection,
  reference: KartFeatureCollection
): FeatureCollectionDiff {
  // require feature ids on both collections
  if (!reference.features.every(hasFeatureId)) {
    throw new Error('All features in the reference FeatureCollection must have an id property.');
  }
  if (!ours.features.every(hasFeatureId)) {
    throw new Error('All features in our FeatureCollection must have an id property.');
  }

  const inserted: FeatureCollectionDiff['inserted'] = [];
  const deleted: FeatureCollectionDiff['deleted'] = [];
  const modified: FeatureCollectionDiff['modified'] = [];

  const referenceFeatureMap = new Map<string, KartEnabledFeature>();
  for (const feature of reference.features) {
    referenceFeatureMap.set(feature.id, feature);
  }

  const oursFeatureMap = new Map<string, KartEnabledFeature>();
  for (const feature of ours.features) {
    oursFeatureMap.set(feature.id, feature);
  }

  for (const [id, ourFeature] of oursFeatureMap) {
    const refFeature = referenceFeatureMap.get(id);
    if (!refFeature) {
      inserted.push(ourFeature);
      continue;
    }

    const hasGeometryChanged =
      serializer.stringify(ourFeature.geometry) !== serializer.stringify(refFeature.geometry);
    const havePropertiesChanged =
      serializer.stringify(ourFeature.properties) !== serializer.stringify(refFeature.properties);
    const havePrimaryKeysChanged =
      serializer.stringify(ourFeature._kart.ids) !== serializer.stringify(refFeature._kart.ids);
    if (!hasGeometryChanged && !havePropertiesChanged && !havePrimaryKeysChanged) {
      continue;
    }

    const modification: (typeof modified)[number] = { id };

    if (hasGeometryChanged) {
      modification.geometry = ourFeature.geometry;
    }

    // for modified properties, only include the properties that have changed
    if (havePropertiesChanged) {
      const modifiedProperties: Record<string, unknown> = {};
      const allKeys = new Set<string>([
        ...Object.keys(refFeature.properties || {}),
        ...Object.keys(ourFeature.properties || {}),
      ]);
      for (const key of allKeys) {
        const refValue = refFeature.properties ? refFeature.properties[key] : undefined;
        const ourValue = ourFeature.properties ? ourFeature.properties[key] : undefined;
        const areDifferent = serializer.stringify(refValue) !== serializer.stringify(ourValue);
        if (areDifferent) {
          modifiedProperties[key] = ourValue;
        }
      }
      modification.properties = modifiedProperties;
    }

    if (havePrimaryKeysChanged) {
      const modifiedIds: Record<string, unknown> = {};
      const allKeys = new Set<string>([
        ...Object.keys(refFeature._kart.ids),
        ...Object.keys(ourFeature._kart.ids),
      ]);
      for (const key of allKeys) {
        const refValue = refFeature._kart.ids[key];
        const ourValue = ourFeature._kart.ids[key];
        const areDifferent = serializer.stringify(refValue) !== serializer.stringify(ourValue);
        if (areDifferent) {
          modifiedIds[key] = ourValue;
        }
      }
      modification.ids = modifiedIds;
    }

    modified.push(modification);
  }

  for (const [id] of referenceFeatureMap) {
    if (!oursFeatureMap.has(id)) {
      deleted.push(id);
    }
  }

  return { inserted, deleted, modified };
}
