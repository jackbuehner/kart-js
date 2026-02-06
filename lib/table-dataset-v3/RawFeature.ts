import * as msgpack from '@msgpack/msgpack';
import { decodeTimestampToTimeSpec, encodeTimeSpecToTimestamp, EXT_TIMESTAMP } from '@msgpack/msgpack';
import { GeoPackageGeometryData } from '@ngageoint/geopackage/dist/lib/geom/geoPackageGeometryData.js';
import { FeatureConverter } from '@ngageoint/simple-features-geojson-js/dist/lib/FeatureConverter.js';
import { Temporal } from 'temporal-polyfill';
import z from 'zod';
import { Enumerable } from '../utils/Enumerable.ts';
import { FileNotFoundError } from '../utils/errors.ts';
import { convertGeometryToWkb, isGeoJsonFeature } from '../utils/features/index.ts';
import { Path } from '../utils/Path.ts';
import type { CRSs } from './CRS.ts';
import { Feature } from './Feature.ts';
import type { Legends } from './Legend.ts';
import type { PathStructure } from './PathStructure.ts';
import type { Schema } from './Schema.ts';

/**
 * A raw feature represents a feature as it is stored on disk in a Kart Table Dataset V3.
 *
 * Raw features store their properties as an array of values, rather than a key-value map.
 * This allows for more efficient storage and retrieval of features.
 *
 * Raw features also reference a legend by its ID, which describes the structure of the feature's properties.
 * This allows features to remain unmodified when the schema changes; the feature's correct
 * structure can always be determined by the permanent registry of current and past legends.
 *
 * To read a feature file, use `RawFeature.fromFile(filePath)`.
 */
export class RawFeature {
  /**
   * The ID of the legend that describes the structure of this feature's properties.
   * This corresponds to the `id` property of a `Legend` instance.
   */
  readonly legendId: string;

  /**
   * The ordered array of primary key values for this feature.
   * The order of the values corresponds to the order of the primary key column IDs in the legend.
   */
  readonly primaryKeys: unknown[];

  /**
   * The non-primary-key properties of the feature.
   * The order of the values corresponds to the order of the non-primary key column IDs in the legend.
   */
  readonly properties: unknown[];

  constructor(legendId: string, primaryKeysData: unknown[], properties: unknown[]) {
    this.legendId = legendId;
    this.primaryKeys = primaryKeysData;
    this.properties = properties;
  }

  /**
   * Upgrades the raw feature to the latest schema and returns
   * it as a structured object of ids (primary keys) and properties (non-primary keys).
   *
   * @throws {Error} If the legend with the specified ID is not found in the provided legends collection.
   *
   * @param legends - The collection of legends to use for upgrading the raw feature.
   * @param schema - The latest schema to upgrade the feature to.
   *
   * @returns An object containing the upgraded ids, properties, and metadata about the upgrade process.
   */
  toObject(legends: Legends, schema: Schema, pathStructure: PathStructure, crss: CRSs) {
    const legend = legends.find((legend) => legend.id === this.legendId);
    if (!legend) {
      throw new Error(`Legend with id ${this.legendId} not found`);
    }

    // use the existing feature legend to create kev-value pairs for the raw feature
    const rawFeatureData = new Map<string, unknown>();
    legend.columnIds.forEach(({ columnId, isPrimary, dataIndex }) => {
      if (isPrimary) {
        rawFeatureData.set(columnId, this.primaryKeys[dataIndex]);
      } else {
        rawFeatureData.set(columnId, this.properties[dataIndex]);
      }
    });

    const processedKeyIds = new Set<string>();

    // Upgrade the primary keys to the current schema.
    // - Primary keys that no longer exist in the schema are dropped.
    // - Primary keys that have been added in the schema are set to null.
    // - Primary keys the were previously null remain null.
    // - Primary keys that were not previously primary are now included.
    const ids = new Map<string, unknown>();
    schema
      .filter((entry) => entry.isPrimary)
      .forEach(({ id, name }) => {
        ids.set(name, rawFeatureData.get(id) ?? null);
        processedKeyIds.add(id);
      });

    // Upgrade the non-primary keys to the current schema.
    // - Non-primary keys that no longer exist in the schema are dropped.
    // - Non-primary keys that have been added in the schema are set to null.
    // - Non-primary keys that were previously null remain null.
    // - Non-primary keys that were previously primary are now included.
    const properties = new Map<string, unknown>();
    schema
      .filter((entry) => !entry.isPrimary)
      .forEach(({ id, name }) => {
        properties.set(name, rawFeatureData.get(id) ?? null);
        processedKeyIds.add(id);
      });

    // The geometry column used for the feature is the first geometry column in the schema.
    // @see https://github.com/koordinates/kart/blob/eae35e1d06273d9cd2638cefd5fdc50250971aa4/kart/sqlalchemy/adapter/gpkg.py#L269-L289
    const featureGeometryColumn = schema.find((entry) => entry.dataType === 'geometry');

    const eid = pathStructure.getEid(Array.from(ids.values()));

    const metadata = {
      /**
       * The primary geometry column used for the feature.
       *
       * This is the first geometry column in the schema or `null` if there is no geometry column.
       */
      geometryColumn: featureGeometryColumn
        ? { id: featureGeometryColumn.id, name: featureGeometryColumn.name }
        : null,
      /**
       * The Coordinate Reference System (CRS) of the feature's geometry.
       * - Any string value is allowed in this field. It must have a corresponding CRS
       *     .wkt file in the dataset's /meta/crs folder that defines defines the WKT
       *     for the CRS.
       * - If the schema does not have a geometry column, this will be null.
       * - If the schema's primary geometry column does not specify a CRS, this will be EPSG:4326.
       * - If the schema's primary geometry column specifies a CRS that cannot be found, this will be null.
       */
      crs: (() => {
        if (!featureGeometryColumn) {
          return null;
        }

        if (!featureGeometryColumn.geometryCrs) {
          return 'EPSG:4326';
        }

        return crss.find((crs) => crs.identifier === featureGeometryColumn.geometryCrs)?.identifier ?? null;
      })(),
      /**
       * An array of column IDs that were present in the legend but not in the current schema.
       * These columns have been dropped during the upgrade process.
       */
      droppedKeys: legend.columnIds
        .filter(({ columnId }) => !processedKeyIds.has(columnId))
        .map(({ columnId }) => columnId),
      /**
       * The encoded ID for the feature, which is determined by the path structure of the dataset and the feature's primary key values.
       * It never starts with a leading slash, and it is relative to the dataset feature storage root.
       */
      eid,
    } as const;

    return {
      ids: ids as ReadonlyMap<string, unknown>,
      properties: properties as ReadonlyMap<string, unknown>,
      metadata,
    };
  }

  toFeature(schema: Schema, legends: Legends, pathStructure: PathStructure, crss: CRSs) {
    return Feature.fromRawFeature(this, schema, legends, pathStructure, crss);
  }

  /**
   * Creates a feature file at the given path.
   *
   * @throws {FileNotFoundError} If the file does not exist at the specified path.
   * @throws {import('../utils/errors.ts').FileReadError} If the file cannot be read.
   * @throws {msgpack.DecodeError} If the feature file name or contents cannot be decoded with MessagePack.
   * @throws {z.ZodError} If the decoded contents are not the correct shape.
   *
   * @param filePath - The path to the feature file.
   */
  static fromFile(filePath: Path) {
    if (!filePath.exists || !filePath.isFile) {
      throw new FileNotFoundError(`File does not exist at path: ${filePath}`);
    }

    const msgpackEncodedName = Uint8Array.fromBase64(filePath.name, { alphabet: 'base64url' });
    const primaryKeyDataDecoded = msgpack.decode(msgpackEncodedName);
    const primaryKeyData = z.unknown().array().parse(primaryKeyDataDecoded);

    const fileContents = filePath.readFileSync();
    const propertiesDecoded = msgpack
      .decodeMulti(fileContents, { extensionCodec, useBigInt64: true })
      .next().value; // decodeMulti allows us to ignore extra trailing bytes
    const [legendId, properties] = rawFeatureFileDataSchema.parse(propertiesDecoded);

    return new RawFeature(legendId, primaryKeyData, properties);
  }
}

export class RawFeatures extends Enumerable<RawFeature> {}

const extensionCodec = new msgpack.ExtensionCodec();
extensionCodec.register({
  type: 71, // 'G'
  encode: (data) => {
    if (!isGeoJsonFeature(data)) {
      throw new Error('Data is not a GeoJSON Feature object.');
    }
    return convertGeometryToWkb(data.geometry);
  },
  decode: (data) => {
    const sfGeom = new GeoPackageGeometryData(data).getOrReadGeometry();
    return FeatureConverter.toFeatureGeometry(sfGeom); // convert to GeoJSON geometry format
  },
});
extensionCodec.register({
  type: EXT_TIMESTAMP,
  encode(input: unknown): Uint8Array | null {
    if (input instanceof Temporal.Instant) {
      const sec = input.epochMilliseconds / 1000;
      const nsec = Number(input.epochNanoseconds % 1_000_000_000n); // we need the remainder nanoseconds after milliseconds
      return encodeTimeSpecToTimestamp({ sec, nsec });
    } else {
      return null;
    }
  },
  decode(data: Uint8Array): Temporal.Instant {
    const timeSpec = decodeTimestampToTimeSpec(data);
    const sec = BigInt(timeSpec.sec);
    const nsec = BigInt(timeSpec.nsec);
    const epochNanoseconds = sec * BigInt(1e9) + nsec;
    return Temporal.Instant.fromEpochNanoseconds(epochNanoseconds);
  },
});

const rawFeatureFileDataSchema = z.tuple([
  z.string(), // legendId
  z.unknown().array(), // non-primary-key properties
]);
