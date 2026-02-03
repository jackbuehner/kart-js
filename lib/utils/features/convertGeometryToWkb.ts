import { GeometryData } from '@ngageoint/geopackage';
import { Envelope } from '@ngageoint/geopackage/dist/lib/geom/envelope.js';
import { bbox as getBoundingBox } from '@turf/bbox';
import wkx from 'wkx';
import { reprojectFeature } from './reprojectFeature.ts';

/**
 * Converts a GeoJSON Geometry to WKB format suitable for storage in a Kart Table V3 dataset.
 *
 * @remarks
 * This function uses the `@ngageoint/geopackage` and `wkx` libraries to perform the conversion.
 *
 * It sets the appropriate byte order (little endian) and SRS ID (0) as required by the Kart specification.
 *
 * It also ensures that the geometry envelope is computed and set for non-point geometries.
 *
 * @param geometry The GeoJSON Geometry to convert.
 * @param toCrs Optional target CRS to reproject the geometry to before conversion. The geometry must have a defined CRS if this is provided.
 * @returns A Uint8Array containing the WKB representation of the geometry.
 */
export function convertGeometryToWkb(geometry: GeoJSON.Geometry, toCrs?: string): Uint8Array {
  // convert GeoJSON geometry to the target CRS if necessary
  const fromCrs = (geometry as any).crs?.properties?.name;
  if (toCrs && !fromCrs) {
    throw new Error('Cannot reproject geometry without a defined CRS.');
  }
  if (fromCrs && toCrs && fromCrs !== toCrs) {
    const t = reprojectFeature({ type: 'Feature', geometry, properties: {} }, toCrs);
    geometry = t.geometry;
  }

  // convert GeoJSON geometry to geopackage geometry
  const wkxGeometry = wkx.Geometry.parseGeoJSON(geometry);
  const geom = new GeometryData();
  geom.setGeometry(wkxGeometry);

  // kart requires little-endian byte order
  geom.byteOrder = GeometryData.LITTLE_ENDIAN;

  // kart requires SRS ID 0 for GeoJSON since the CRS is defined at the dataset level
  geom.setSrsId(0);

  // for non-point geometries, compute and set the envelope (required by Kart)
  // see https://docs.kartproject.org/en/latest/pages/development/table_v3.html#:~:text=Geometries%20are%20encoded,the%20geometryCRS%20field.
  if (geometry.type !== 'Point') {
    const bbox = getBoundingBox(geometry, { recompute: true });

    const envelope = new Envelope();
    envelope.minX = bbox[0];
    envelope.minY = bbox[1];
    envelope.maxX = bbox[2];
    envelope.maxY = bbox[3];
    envelope.hasZ = false;
    envelope.hasM = false;

    geom.setEnvelope(envelope);
  }

  return new Uint8Array(geom.toData());
}
