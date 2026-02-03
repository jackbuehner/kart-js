import type { GeometryWithCrs } from './index.ts';
import { transformCoordinates } from './transformCoordinates.ts';

/**
 * Converts a GeoJSON feature from its current CRS to the specified CRS.
 *
 * If a feature does not have a CRS defined, it is assumed to be in EPSG:4326 (WGS 84).
 */
export function reprojectFeature<T extends GeometryWithCrs, K extends GeoJSON.GeoJsonProperties>(
  feature: GeoJSON.Feature<T, K>,
  toCrs: string
): GeoJSON.Feature<T, K> {
  // per spec, GeoJSON without a CRS is assumed to be EPSG:4326 (WGS 84)
  const fromCrs = feature.geometry.crs?.properties.name || 'EPSG:4326';

  // if the from and to CRS are the same, no need to transform
  if (fromCrs === toCrs) {
    return feature;
  }

  if (feature.geometry.type === 'GeometryCollection') {
    return {
      ...feature,
      geometry: {
        ...feature.geometry,
        geometries: feature.geometry.geometries.map(
          (geometry) =>
            reprojectFeature({ ...feature, geometry } as GeoJSON.Feature<GeometryWithCrs, K>, toCrs).geometry
        ),
      },
    };
  }

  return {
    ...feature,
    geometry: {
      ...feature.geometry,
      crs: { type: 'name', properties: { name: toCrs } },
      coordinates: transformCoordinates(feature.geometry.coordinates, fromCrs, toCrs),
    },
  };
}
