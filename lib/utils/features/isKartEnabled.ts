import type { GeoJsonProperties, Geometry } from 'geojson';
import { hasFeatureId, type FeatureWithId } from './hasFeatureId.ts';
import { isGeoJsonFeature } from './isGeoJsonFeature.ts';

/**
 * Whether the given feature is a Kart-enabled feature.
 */
export function isKartEnabledFeature(toCheck: unknown): toCheck is KartFeatureCollection['features'][number] {
  return isGeoJsonFeature(toCheck) && hasFeatureId(toCheck);
}

export type GeometryWithCrs<T extends Geometry = Geometry> = T & {
  crs?: { type: 'name'; properties: { name: string } };
};

export interface KartFeatureCollection<
  G extends GeometryWithCrs = GeometryWithCrs,
  P = GeoJsonProperties,
> extends GeoJSON.FeatureCollection<G, P> {
  features: KartEnabledFeature<G, P>[];
}

export type KartEnabledFeature<
  G extends GeometryWithCrs = GeometryWithCrs,
  P = GeoJsonProperties,
> = FeatureWithId<G, P>;
