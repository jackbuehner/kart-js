import * as msgpack from '@msgpack/msgpack';
import { decodeTimestampToTimeSpec, encodeTimeSpecToTimestamp, EXT_TIMESTAMP } from '@msgpack/msgpack';
import { GeometryData } from '@ngageoint/geopackage';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { inspect, type InspectOptions } from 'node:util';
import { Temporal } from 'temporal-polyfill';
import z from 'zod';
import type { FeatureWithId } from '../utils/features/index.ts';
import {
  convertGeometryToWkb,
  isGeoJsonFeature,
  reprojectFeature,
  type GeometryWithCrs,
} from '../utils/features/index.ts';
import { decodeRawFeatures } from './decodeRawFeatures.ts';
import { legendSchema, schemaEntrySchema } from './schemas/table-dataset-v3.ts';
import { makeSerializeable } from './serializer.ts';
import { WorkingFeatureCollection } from './WorkingFeatureCollection.ts';

export class TableDatasetV3 {
  #datasetPath: string;

  readonly type = 'table-dataset-v3';
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  readonly pathStructure: z.infer<typeof pathStructureSchema>;
  readonly schema: z.infer<typeof schemaEntrySchema>[];
  readonly legends: Record<string, z.infer<typeof legendSchema>>;

  readonly working: WorkingFeatureCollection;

  constructor(repoPath: string, id: string) {
    if (!TableDatasetV3.isValidDataset(repoPath, id)) {
      throw new Error(`Dataset with id "${id}" does not exist or is not a valid table dataset v3.`);
    }

    this.#datasetPath = path.join(repoPath, id);
    this.id = id;

    try {
      const validatedContents = TableDatasetV3.getValidatedContents(repoPath, id);
      if (!validatedContents) {
        throw new Error('Could not validate dataset contents.');
      }
      this.title = validatedContents.title;
      this.pathStructure = validatedContents.pathStructure;
      this.schema = validatedContents.schema;
      this.description = validatedContents.description;
      this.legends = validatedContents.legends;
    } catch (error) {
      throw new Error(`Dataset with id "${id}" has invalid contents: ${(error as Error).message}`);
    }

    this.working = new WorkingFeatureCollection(this.id, this.toFeatureCollection(), this.schema);
  }

  static isValidDataset(repoDir: string, id: string, validateContents = false) {
    const folderExists = readdirSync(repoDir).findIndex((file) => file === id) !== -1;
    if (!folderExists) {
      return false;
    }

    // table datasets MUST have a .table-dataset folder inside their root folder that contains at least the feature and meta folders
    const tableDatasetPath = path.join(repoDir, id, '.table-dataset');
    if (!existsSync(tableDatasetPath)) {
      return false;
    }

    const tableDatasetContents = readdirSync(tableDatasetPath);
    const hasMetaFolder = tableDatasetContents.findIndex((file) => file === 'meta') !== -1;
    if (!hasMetaFolder) {
      return false;
    }

    const metaFolderContents = readdirSync(path.join(tableDatasetPath, 'meta'));
    const hasTitleFile = metaFolderContents.findIndex((file) => file === 'title') !== -1;
    const hasSchemaFile = metaFolderContents.findIndex((file) => file === 'schema.json') !== -1;
    const hasPathStructureFile = metaFolderContents.findIndex((file) => file === 'path-structure.json') !== -1;
    if (!hasTitleFile || !hasSchemaFile || !hasPathStructureFile) {
      return false;
    }

    const hasLegendFolder = metaFolderContents.findIndex((file) => file === 'legend') !== -1;
    if (!hasLegendFolder) {
      return false;
    }
    const hasAtLeastOneLegendFile = readdirSync(path.join(tableDatasetPath, 'meta', 'legend')).length > 0;
    if (!hasAtLeastOneLegendFile) {
      return false;
    }

    if (!validateContents) {
      return true;
    }

    try {
      this.getValidatedContents(repoDir, id);
      return true;
    } catch {
      return false;
    }
  }

  private static getValidatedContents(repoDir: string, id: string) {
    if (!TableDatasetV3.isValidDataset(repoDir, id, false)) {
      return;
    }

    const titleFilePath = path.join(repoDir, id, '.table-dataset', 'meta', 'title');
    const title = readFileSync(titleFilePath, { encoding: 'utf-8' }).trim();

    const descriptionFilePath = path.join(repoDir, id, '.table-dataset', 'meta', 'description');
    let description: string | undefined = undefined;
    if (existsSync(descriptionFilePath)) {
      description = readFileSync(descriptionFilePath, { encoding: 'utf-8' }).trim();
    }

    const pathStructurePath = path.join(repoDir, id, '.table-dataset', 'meta', 'path-structure.json');
    const pathStructureRaw = readFileSync(pathStructurePath, { encoding: 'utf-8' });
    const pathStructure = pathStructureSchema.parse(JSON.parse(pathStructureRaw));

    const schemaFilePath = path.join(repoDir, id, '.table-dataset', 'meta', 'schema.json');
    const schemaRaw = readFileSync(schemaFilePath, { encoding: 'utf-8' });
    const schema = z.array(schemaEntrySchema).parse(JSON.parse(schemaRaw));

    const legendDirPath = path.join(repoDir, id, '.table-dataset', 'meta', 'legend');
    const legendFiles = readdirSync(legendDirPath);
    const legends: Record<string, z.infer<typeof legendSchema>> = {};
    for (const legendFile of legendFiles) {
      const legendFilePath = path.join(legendDirPath, legendFile);
      const legendRaw = readFileSync(legendFilePath);
      const legendData = msgpack.decode(new Uint8Array(legendRaw), { extensionCodec, useBigInt64: true });
      const legendName = path.parse(legendFile).name;
      legends[legendName] = legendSchema.parse(legendData);
    }

    return {
      title,
      description,
      pathStructure,
      schema,
      legends,
    };
  }

  /**
   * Gets each feature inside the feature folder for the dataset
   * in its raw form.
   */
  protected discoverRawFeatures() {
    const featureDirPath = path.join(this.#datasetPath, '.table-dataset', 'feature');
    if (!existsSync(featureDirPath)) {
      return []; // a missing folder indicates no features
    }

    const featureFiles = readdirSync(featureDirPath, { recursive: true, withFileTypes: true });

    return featureFiles
      .filter((file) => file.isFile())
      .map((file) => {
        const filePath = path.join(file.parentPath, file.name);
        const fileContents = readFileSync(filePath);
        const fileData = msgpack.decode(new Uint8Array(fileContents), { extensionCodec, useBigInt64: true });

        if (!Array.isArray(fileData) || fileData.length !== 2) {
          throw new Error(`Feature file at path "${filePath}" is not a valid feature.`);
        }

        if (typeof fileData[0] !== 'string') {
          throw new Error(`Feature file at path "${filePath}" does not have a valid legend id.`);
        }

        if (!Array.isArray(fileData[1])) {
          throw new Error(`Feature file at path "${filePath}" does not have valid feature data.`);
        }

        // the file name is a base64-encoed msgpack-encoded array of the primary key values (in order)
        const msgpackEncodedName = Buffer.from(path.parse(file.name).name, 'base64');
        const primaryKeyData = msgpack.decode(msgpackEncodedName) as unknown[];

        // the other values are stored directly in the data array
        const nonPrimaryKeyData = fileData[1] as unknown[];

        const pathAfterFeatureDir = path.relative(featureDirPath, filePath).replaceAll(path.sep, '/');

        const legend = this.legends[fileData[0]];
        if (!legend) {
          throw new Error(`Legend with id "${fileData[0]}" not found in dataset legends.`);
        }

        // use the legend to construct the actual data object with proper column names
        // TODO: deserialize data types properly based on schema (e..g blob, decimal)... maybe modify msgpack decoder to handle this?
        const properties: Record<string, unknown> = {};
        legend.primaryKeyColumns.forEach((columnId, index) => {
          const columnName = this.schema.find(({ id }) => id === columnId)?.name || columnId;
          properties[columnName] = primaryKeyData[index];
        });
        legend.nonPrimaryKeyColumns.forEach((columnId, index) => {
          const columnName = this.schema.find(({ id }) => id === columnId)?.name || columnId;
          properties[columnName] = nonPrimaryKeyData[index];
        });

        const primaryKeyNames = legend.primaryKeyColumns.map((columnId) => {
          return this.schema.find(({ id }) => id === columnId)?.name || columnId;
        });

        const geometryColumns = this.schema
          .filter(({ dataType }) => dataType === 'geometry')
          .map(({ name }) => name);
        const primaryGeometryKey = geometryColumns.includes('geometry')
          ? 'geometry'
          : geometryColumns.includes('geom')
            ? 'geom'
            : geometryColumns[0];

        // construct a json schema that can be used by clients that
        // need to understand the constraints on each property
        const schema = {
          $schema: 'https://json-schema.org/draft/2020-12/schema' as const,
          type: 'object' as const,
          properties: Object.fromEntries(
            [...legend.primaryKeyColumns, ...legend.nonPrimaryKeyColumns]
              .map((columnId): [string, JsonSchemaDataTypes] | undefined => {
                const columnSchema = this.schema.find(({ id }) => id === columnId);
                if (!columnSchema) {
                  return;
                }
                const key = columnSchema.name || columnId;

                if (columnSchema.dataType === 'blob') {
                  return [
                    key,
                    {
                      type: 'array',
                      items: { type: 'integer', minimum: 0, maximum: 255 },
                      format: 'bytes',
                    },
                  ];
                }

                if (columnSchema.dataType === 'boolean') {
                  return [key, { type: 'boolean' }];
                }

                if (columnSchema.dataType === 'date') {
                  return [key, { type: 'string', format: 'date' }];
                }

                if (columnSchema.dataType === 'float') {
                  return [
                    key,
                    {
                      type: 'number',
                      minimum: columnSchema.size === 32 ? MIN_32_BIT_FLOAT : MIN_64_BIT_FLOAT,
                      maximum: columnSchema.size === 32 ? MAX_32_BIT_FLOAT : MAX_64_BIT_FLOAT,
                    },
                  ];
                }

                if (columnSchema.dataType === 'geometry') {
                  return [
                    key,
                    {
                      type: 'object',
                      $ref: 'https://geojson.org/schema/Geometry.json',
                    },
                  ];
                }

                if (columnSchema.dataType === 'integer') {
                  return [
                    key,
                    {
                      type: 'integer',
                      minimum: BigInt(
                        columnSchema.size === 8
                          ? MIN_INT8
                          : columnSchema.size === 16
                            ? MIN_INT16
                            : columnSchema.size === 32
                              ? MIN_INT32
                              : MIN_INT64
                      ),
                      maximum: BigInt(
                        columnSchema.size === 8
                          ? MAX_INT8
                          : columnSchema.size === 16
                            ? MAX_INT16
                            : columnSchema.size === 32
                              ? MAX_INT32
                              : MAX_INT64
                      ),
                    },
                  ];
                }

                if (columnSchema.dataType === 'interval') {
                  return [
                    key,
                    {
                      type: 'string',
                      // ISO 8601 duration format
                      pattern: `^P(\\d+Y)?(\\d+M)?(\\d+D)?(T(\\d+H)?(\\d+M)?(\\d+(\\.\\d+)?S)?)?$`,
                      format: 'duration',
                    },
                  ];
                }

                if (columnSchema.dataType === 'numeric') {
                  return [
                    key,
                    {
                      type: 'string',
                      // optional negative sign + digits before decimal + decimal + digits after decimal
                      pattern: `^-?\\d{1,${columnSchema.precision - columnSchema.scale}}(\\.\\d{1,${columnSchema.scale}})?$`,
                      format: 'decimal',
                      'x-precision': columnSchema.precision,
                      'x-scale': columnSchema.scale,
                    },
                  ];
                }

                if (columnSchema.dataType === 'text') {
                  return [
                    key,
                    {
                      type: 'string',
                      maxLength: typeof columnSchema.length === 'number' ? columnSchema.length : undefined,
                    },
                  ];
                }

                if (columnSchema.dataType === 'time') {
                  return [key, { type: 'string', format: 'time' }];
                }

                if (columnSchema.dataType === 'timestamp') {
                  return [key, { type: 'string', format: 'date-time' }];
                }
              })
              .filter((x) => !!x)
          ),
        };

        // assume EPSG:4326 for geometries if no CRS is specified in the schema
        const geometryCrs =
          this.schema
            .filter((entry) => entry.dataType === 'geometry')
            .find(({ name }) => name === primaryGeometryKey)?.geometryCrs ?? 'EPSG:4326';

        return {
          path: pathAfterFeatureDir,
          legendId: fileData[0],
          primaryKeys: primaryKeyNames,
          primaryGeometryKey,
          schema,
          properties,
          crs: geometryCrs,
        };
      });
  }

  /**
   * Gets each feature inside the feature folder for the dataset
   * inside it decoded and validated form.
   */
  toDecodedFeatures() {
    const rawFeatures = this.discoverRawFeatures();
    return decodeRawFeatures(rawFeatures);
  }

  toFeatureCollection() {
    return makeSerializeable({
      type: 'FeatureCollection',
      features: this.toDecodedFeatures()
        .map((feature) => {
          let pkey = feature.properties[feature.primaryKeys[0]];
          let id: string | number;
          if (typeof pkey === 'string' || typeof pkey === 'number') {
            id = pkey;
          } else {
            // primary keys MUST be strings or numbers to be used as GeoJSON feature ids
            if (typeof pkey === 'bigint') {
              id = pkey.toString() + 'n';
            } else {
              throw new Error(
                `Feature primary key "${feature.primaryKeys[0]}" in dataset "${this.id}" must be a string or number to be used as a GeoJSON feature id.`
              );
            }
          }

          const _geometry = feature.properties[feature.primaryGeometryKey];
          if (_geometry === null || _geometry === undefined) {
            return;
          }
          if (typeof _geometry !== 'object' || (_geometry as GeoJSON.Geometry).type === undefined) {
            console.error('Invalid geometry property:', _geometry);
            throw new Error(
              `Feature with id "${id}" in dataset "${this.id}" does not have a valid geometry property "${feature.primaryGeometryKey}".`
            );
          }
          const geometry = _geometry as GeometryWithCrs;
          geometry.crs = { type: 'name', properties: { name: feature.crs } };

          const properties = { ...feature.properties };
          delete properties[feature.primaryKeys[0]];
          delete properties[feature.primaryGeometryKey];

          return reprojectFeature(
            {
              type: 'Feature',
              id,
              geometry,
              properties,
            },
            'EPSG:4326'
          ) as FeatureWithId<GeometryWithCrs>;
        })
        .filter((x) => !!x),
    }) satisfies GeoJSON.FeatureCollection<GeoJSON.Geometry>;
  }
}

const MIN_32_BIT_FLOAT = -3.4e38;
const MAX_32_BIT_FLOAT = 3.4e38;
const MIN_64_BIT_FLOAT = -1.7e308;
const MAX_64_BIT_FLOAT = 1.7e308;

const MAX_INT8 = 127;
const MIN_INT8 = -128;
const MAX_INT16 = 32_767;
const MIN_INT16 = -32_768;
const MAX_INT32 = 2_147_483_647;
const MIN_INT32 = -2_147_483_648;
const MAX_INT64 = 9_223_372_036_854_775_807n;
const MIN_INT64 = -9_223_372_036_854_775_808n;

export type JsonSchemaDataTypes =
  | BlobSchema
  | BooleanSchema
  | DateSchema
  | FloatSchema
  | GeometrySchema
  | IntegerSchema
  | IntervalSchema
  | NumericSchema
  | TextSchema
  | TimeSchema
  | TimestampSchema;

interface BlobSchema {
  type: 'array';
  items: {
    type: 'integer';
    minimum: 0;
    maximum: 255;
  };
  format: 'bytes';
}

interface BooleanSchema {
  type: 'boolean';
}

interface DateSchema {
  type: 'string';
  format: 'date';
}

interface FloatSchema {
  type: 'number';
  minimum: number;
  maximum: number;
}

interface GeometrySchema {
  type: 'object';
  $ref: 'https://geojson.org/schema/Geometry.json';
}

interface IntegerSchema {
  type: 'integer';
  minimum: bigint;
  maximum: bigint;
}

interface IntervalSchema {
  type: 'string';
  pattern: '^P(\\d+Y)?(\\d+M)?(\\d+D)?(T(\\d+H)?(\\d+M)?(\\d+(\\.\\d+)?S)?)?$';
  format: 'duration';
}

interface NumericSchema {
  type: 'string';
  pattern: `^-?\\d{1,${number}}(\\.\\d{1,${number}})?$`;
  format: 'decimal';
  'x-precision': number;
  'x-scale': number;
}

interface TextSchema {
  type: 'string';
  maxLength?: number;
}

interface TimeSchema {
  type: 'string';
  format: 'time';
}

interface TimestampSchema {
  type: 'string';
  format: 'date-time';
}

const pathStructureSchema = z
  .object({
    scheme: z.literal('int').or(z.literal('msgpack/hash')),
    branches: z.literal(16).or(z.literal(64)).or(z.literal(256)),
    levels: z.int(),
    encoding: z.literal('base64').or(z.literal('hex')),
  })
  .superRefine((data, ctx) => {
    if (data.encoding === 'base64' && data.branches !== 64) {
      ctx.addIssue({
        code: 'custom',
        path: ['branches'],
        message: 'When encoding is base64, branches must be 64.',
      });
    }

    if (data.encoding === 'hex' && ![16, 256].includes(data.branches)) {
      ctx.addIssue({
        code: 'custom',
        path: ['branches'],
        message: 'When encoding is hex, branches must be either 16 or 256.',
      });
    }
  });

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
    const geom = new GeometryData(data);
    return geom.toGeoJSON();
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

// ensure that the temporal dates print nicely in the console

(Temporal.PlainDate.prototype as any)[inspect.custom] = function (
  this: Temporal.PlainDate,
  depth: number,
  opts: InspectOptions
) {
  return (opts as any).stylize(this.toString(), 'date');
};
(Temporal.Instant.prototype as any)[inspect.custom] = function (
  this: Temporal.Instant,
  depth: number,
  opts: InspectOptions
) {
  return (opts as any).stylize(this.toString(), 'date');
};
(Temporal.PlainTime.prototype as any)[inspect.custom] = function (
  this: Temporal.PlainTime,
  depth: number,
  opts: InspectOptions
) {
  return (opts as any).stylize(this.toString(), 'date');
};
(Temporal.PlainDateTime.prototype as any)[inspect.custom] = function (
  this: Temporal.PlainDateTime,
  depth: number,
  opts: InspectOptions
) {
  return (opts as any).stylize(this.toString(), 'date');
};
