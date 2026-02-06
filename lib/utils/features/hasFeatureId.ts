import type { Feature, GeoJsonProperties, Geometry } from 'geojson';

/**
 * A type guard to check if a GeoJSON Feature has an 'id' property.
 */
export function hasFeatureId(feature: GeoJSON.Feature): feature is FeatureWithId {
  return feature.id !== undefined && feature.id !== null;
}

export type FeatureWithId<G extends Geometry | null = Geometry, P = GeoJsonProperties> = Feature<G, P> & {
  id: string;
};
