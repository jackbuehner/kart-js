import proj4 from 'proj4';

type Coordinates =
  | GeoJSON.Point['coordinates']
  | GeoJSON.MultiPoint['coordinates']
  | GeoJSON.LineString['coordinates']
  | GeoJSON.MultiLineString['coordinates']
  | GeoJSON.Polygon['coordinates']
  | GeoJSON.MultiPolygon['coordinates'];
type NestedCoordinates =
  | GeoJSON.MultiPoint['coordinates']
  | GeoJSON.LineString['coordinates']
  | GeoJSON.MultiLineString['coordinates']
  | GeoJSON.Polygon['coordinates']
  | GeoJSON.MultiPolygon['coordinates'];

/**
 * Transforms GeoJSON coordinates from one CRS to another. The CRS must be a valid EPSG code.
 */
export function transformCoordinates(coords: Coordinates, fromCrs: string, toCrs: string): Coordinates {
  if (Array.isArray(coords) && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
    // base case: [x, y]
    return proj4(fromCrs, toCrs, coords as [number, number]);
  }

  // otherwise recurse through deeper nested coordinate arrays
  return (coords as NestedCoordinates).map((c) =>
    transformCoordinates(c as Coordinates, fromCrs, toCrs)
  ) as Coordinates;
}
