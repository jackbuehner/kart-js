import type { FeatureWithId } from './index.ts';

/**
 * A type guard to check if a GeoJSON Feature has an 'id' property.
 */
export function hasFeatureId(feature: GeoJSON.Feature): feature is FeatureWithId {
  return feature.id !== undefined && feature.id !== null;
}
