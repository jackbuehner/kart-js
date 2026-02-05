import { GeoPackageGeometryData } from '@ngageoint/geopackage/dist/lib/geom/geoPackageGeometryData.js';
import { FeatureConverter } from '@ngageoint/simple-features-geojson-js';
import { ByteOrder } from '@ngageoint/simple-features-wkb-js';
import { CRS, CRSs } from '../../table-dataset-v3/CRS.ts';
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
export function convertGeometryToWkb(geometry: GeoJSON.Geometry, toCrs?: CRS | string): Uint8Array {
  // convert GeoJSON geometry to the target CRS if necessary
  const fromCrs = (geometry as any).crs?.properties?.name;
  if (toCrs && !fromCrs) {
    throw new Error('Cannot reproject geometry without a defined CRS.');
  }
  if (fromCrs && toCrs && fromCrs !== toCrs) {
    const toCRS = typeof toCrs === 'string' ? toCrs : toCrs.identifier;
    const t = reprojectFeature(
      { type: 'Feature', geometry, properties: {} },
      toCRS,
      toCrs instanceof CRS ? new CRSs([toCrs]) : undefined
    );
    geometry = t.geometry;
  }

  // convert GeoJSON geometry to geopackage geometry
  const sfGeom = FeatureConverter.toSimpleFeaturesGeometryFromGeometryObject(geometry);
  const geom = GeoPackageGeometryData.create(sfGeom); // this static method creates without building the envelope
  geom.setGeometry(sfGeom);

  // kart requires little-endian byte order
  geom.setByteOrder(ByteOrder.LITTLE_ENDIAN);

  // kart requires SRS ID 0 for GeoJSON since the CRS is defined at the dataset level
  geom.setSrsId(0);

  // for non-point geometries, compute and set the envelope (required by Kart)
  // see https://docs.kartproject.org/en/latest/pages/development/table_v3.html#:~:text=Geometries%20are%20encoded,the%20geometryCRS%20field.
  if (geometry.type !== 'Point') {
    geom.setEnvelope(sfGeom.getEnvelope());
  }

  return new Uint8Array(geom.toBuffer()).slice();
}

// class GeoPackageBinaryHeader {
//   // "GP" in ASCII
//   magic = 0x4750

//   // 0 = version 1
//   version = 0

//   // bit 7: 0 - reserved
//   // bit 6: 0 - reserveed
//   // bit 5: 0 - StandardGeoPackageBinary
//   // bit 4: empty geometry flag: 0 - not empty, 1 - empty
//   // bits 3-1: envelope contents indicator code (3-bit unisgned int):
//   //   0: no envelope
//   //   1: envelope is [minx, maxx, miny, maxy]
//   //   2: envelope is [minx, maxx, miny, maxy, minz, maxz], 48 bytes
//   //   3: envelope is [minx, maxx, miny, maxy, minm, maxm], 48 bytes
//   //   4: envelope is [minx, maxx, miny, maxy, minz, maxz, minm, maxm], 64 bytes
//   //   5-7: invalid
//   // bit 0: header byte order: 0 - little endian, 1 - big endian
//   flags = 0b00000000

//   // Kart always uses 0 for SRS/CRS since the CRS is defined at the dataset level
//   srs_id = 0

//   envelope?: Envelope;

//   private setEnvelopeType(type: 'none' |'xy' |'xyz' | 'xym' | 'xyzm') {
//     const envelopeIndicator = {
//       'none': 0,
//       'xy': 1,
//       'xyz': 2,
//       'xym': 3,
//       'xyzm': 4,
//     }[type];
//     this.flags = (this.flags & 0b11110000) | (envelopeIndicator << 1);
//   }

//   setEmptyGeometry(isEmpty: boolean) {
//     if (isEmpty) {
//       this.flags = this.flags | 0b00010000;
//     } else {
//       this.flags = this.flags & 0b11101111;
//     }
//   }

//   setEnvelope(envelope: Envelope) {
//     this.envelope = envelope;
//     if (envelope.hasZ && envelope.hasM) {
//       this.setEnvelopeType('xyzm');
//     } else if (envelope.hasZ) {
//       this.setEnvelopeType('xyz');
//     } else if (envelope.hasM) {
//       this.setEnvelopeType('xym');
//     } else {
//       this.setEnvelopeType('xy');
//     }
//   }

//   toBuffer() {
//     const buffer = new ArrayBuffer(8 + (this.envelope ? this.envelope.toBuffer().byteLength : 0));
//     const view = new DataView(buffer);

//     // write header
//     view.setUint16(0, this.magic, true);
//     view.setUint8(2, this.version);
//     view.setUint8(3, this.flags);
//     view.setUint32(4, this.srs_id, true);

//     // write envelope if present
//     if (this.envelope) {
//       const envelopeBuffer = this.envelope.toBuffer();
//       new Uint8Array(buffer).set(new Uint8Array(envelopeBuffer), 8);
//     }
//   }
// }

// class StandardGeoPackageBinary {
//   header: GeoPackageBinaryHeader;
//   geometry: WKBGeometry;

//   constructor(geometry: GeoJSON.Geometry) {
//     this.header = new GeoPackageBinaryHeader();
//   }
// }

// class WKBGeometry {

// }

// class Envelope {
//   minX: number;
//   maxX: number
//   minY: number;
//   maxY: number;
//   minZ?: number
//   maxZ?: number;
//   minM?: number
//   maxM?: number;

//   constructor(minX: number, maxX: number, minY: number, maxY: number, minZ?: number, maxZ?: number, minM?: number, maxM?: number) {
//     this.minX = minX;
//     this.maxX = maxX;
//     this.minY = minY;
//     this.maxY = maxY;
//     this.minZ = minZ;
//     this.maxZ = maxZ;
//     this.minM = minM;
//     this.maxM = maxM;
//   }

//   get hasZ() {
//     return this.minZ !== undefined && this.maxZ !== undefined;
//   }

//   get hasM() {    return this.minM !== undefined && this.maxM !== undefined;
//   }

//   /**
//    * Writes the envelope to an ArrayBuffer in the format required by the GeoPackage binary encoding.
//    */
//   toBuffer() {
//     const buffer = new ArrayBuffer(64); // max size needed for xyzm envelope
//     const view = new DataView(buffer);
//     let offset = 0;

//     view.setFloat64(offset, this.minX, true); offset += 8;
//     view.setFloat64(offset, this.maxX, true); offset += 8;
//     view.setFloat64(offset, this.minY, true); offset += 8;
//     view.setFloat64(offset, this.maxY, true); offset += 8;

//     if (this.hasZ) {
//       view.setFloat64(offset, this.minZ!, true); offset += 8;
//       view.setFloat64(offset, this.maxZ!, true); offset += 8;
//     }

//     if (this.hasM) {
//       view.setFloat64(offset, this.minM!, true); offset += 8;
//       view.setFloat64(offset, this.maxM!, true); offset += 8;
//     }

//     return buffer
//   }
// }
