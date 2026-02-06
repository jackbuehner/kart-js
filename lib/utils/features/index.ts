export { convertGeometryToWkb } from './convertGeometryToWkb.ts';
export { diffFeatureCollections } from './diffFeatureCollections.ts';
export { hasFeatureId } from './hasFeatureId.ts';
export { isGeoJsonFeature } from './isGeoJsonFeature.ts';
export { isKartEnabledFeature } from './isKartEnabled.ts';
export { reprojectFeature } from './reprojectFeature.ts';
export { transformCoordinates } from './transformCoordinates.ts';

import type { Feature, GeoJsonProperties, Geometry } from 'geojson';

export type GeometryWithCrs = GeoJSON.Geometry & {
  crs?: { type: 'name'; properties: { name: string } };
};

export type FeatureWithId<G extends Geometry | null = Geometry, P = GeoJsonProperties> = Feature<G, P> & {
  id: string | number;
};

export type KartEnabledFeature<G extends Geometry | null = Geometry, P = GeoJsonProperties> = Feature<G, P> & {
  id: string;
  _kart: {
    ids: { [k: string]: unknown };
    eid: string;
    geometryColumn: {
      id: string;
      name: string;
    };
  };
};

export interface KartFeatureCollection<
  G extends Geometry | null = Geometry,
  P = GeoJsonProperties,
> extends GeoJSON.FeatureCollection<G, P> {
  features: KartEnabledFeature<G, P>[];
}

const t = Object.freeze({});
