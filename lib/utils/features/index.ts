export { convertGeometryToWkb } from './convertGeometryToWkb.ts';
export { diffFeatureCollections } from './diffFeatureCollections.ts';
export { hasFeatureId } from './hasFeatureId.ts';
export { isGeoJsonFeature } from './isGeoJsonFeature.ts';
export { reprojectFeature } from './reprojectFeature.ts';
export { transformCoordinates } from './transformCoordinates.ts';

// register all EPSG codes
import epsg from 'epsg-index/all.json' with { type: 'json' };
import proj4 from 'proj4';
for (const code in epsg) {
  proj4.defs(`EPSG:${code}`, (epsg as any)[code].proj4);
}

import type { Feature, GeoJsonProperties, Geometry } from 'geojson';

export type GeometryWithCrs = GeoJSON.Geometry & { crs?: { type: 'name'; properties: { name: string } } };

export type FeatureWithId<G extends Geometry | null = Geometry, P = GeoJsonProperties> = Feature<G, P> & {
  id: string | number;
};

export type KartEnabledFeature<G extends Geometry | null = Geometry, P = GeoJsonProperties> = Feature<G, P> & {
  id: string;
  _kart: {
    ids: { [k: string]: unknown };
    path: string;
    geometryColumn: {
      id: string;
      name: string;
    };
  };
};
