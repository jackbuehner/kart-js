import * as msgpack from '@msgpack/msgpack';
import { decodeTimestampToTimeSpec, encodeTimeSpecToTimestamp, EXT_TIMESTAMP } from '@msgpack/msgpack';
import { GeoPackageGeometryData } from '@ngageoint/geopackage/dist/lib/geom/geoPackageGeometryData.js';
import { FeatureConverter } from '@ngageoint/simple-features-geojson-js';
import { Temporal } from 'temporal-polyfill';
import z from 'zod';
import type { FeatureWithId } from '../utils/features/index.ts';
import {
  convertGeometryToWkb,
  isGeoJsonFeature,
  reprojectFeature,
  type GeometryWithCrs,
} from '../utils/features/index.ts';
import { Path } from '../utils/index.ts';
import { decodeRawFeatures } from './decodeRawFeatures.ts';
import { Legend, Legends } from './Legend.ts';
import { PathStructure } from './PathStructure.ts';
import { RawFeature } from './RawFeature.ts';
import { Schema } from './Schema.ts';
import { makeSerializeable } from './serializer.ts';
import { WorkingFeatureCollection } from './WorkingFeatureCollection.ts';

export class TableDatasetV3 {
  #datasetPath: Path;

  readonly type = 'table-dataset-v3';
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  readonly pathStructure: PathStructure;
  readonly schema: Schema;
  readonly legends: Legends;

  readonly working: WorkingFeatureCollection;

  constructor(repoPath: Path, id: string) {
    if (!TableDatasetV3.isValidDataset(repoPath, id)) {
      throw new Error(`Dataset with id "${id}" does not exist or is not a valid table dataset v3.`);
    }

    this.#datasetPath = repoPath.join(id);
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
      const toThrow = new Error(`Dataset with id "${id}" has invalid contents: ${(error as Error).message}`);
      if (error instanceof Error) {
        toThrow.stack = error.stack;
        toThrow.cause = error.cause;
        toThrow.name = error.name;
      }
      throw toThrow;
    }

    this.working = new WorkingFeatureCollection(this.id, this.toFeatureCollection(), this.schema);
  }

  static isValidDataset(repoDir: Path, id: string, validateContents = false) {
    const folderExists = repoDir.readDirectorySync().findIndex((item) => item.name === id) !== -1;
    if (!folderExists) {
      return false;
    }

    // table datasets MUST have a .table-dataset folder inside their root folder that contains at least the feature and meta folders
    const tableDatasetPath = repoDir.join(id, '.table-dataset');
    if (!tableDatasetPath.exists) {
      return false;
    }

    const tableDatasetContents = tableDatasetPath.readDirectorySync();
    const hasMetaFolder = tableDatasetContents.findIndex((item) => item.name === 'meta') !== -1;
    if (!hasMetaFolder) {
      return false;
    }

    const metaFolderContents = tableDatasetPath.join('meta').readDirectorySync();
    const hasTitleFile = metaFolderContents.findIndex((file) => file.name === 'title') !== -1;
    const hasSchemaFile = metaFolderContents.findIndex((file) => file.name === 'schema.json') !== -1;
    const hasPathStructureFile =
      metaFolderContents.findIndex((file) => file.name === 'path-structure.json') !== -1;
    if (!hasTitleFile || !hasSchemaFile || !hasPathStructureFile) {
      return false;
    }

    const hasLegendFolder = metaFolderContents.findIndex((file) => file.name === 'legend') !== -1;
    if (!hasLegendFolder) {
      return false;
    }
    const hasAtLeastOneLegendFile = tableDatasetPath.join('meta', 'legend').readDirectorySync().length > 0;
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

  private static getValidatedContents(repoDir: Path, id: string) {
    if (!TableDatasetV3.isValidDataset(repoDir, id, false)) {
      return;
    }

    const titleFilePath = repoDir.join(id, '.table-dataset', 'meta', 'title');
    const title = titleFilePath.readFileSync({ encoding: 'utf-8' }).trim();

    const descriptionFilePath = repoDir.join(id, '.table-dataset', 'meta', 'description');
    let description: string | undefined = undefined;
    if (descriptionFilePath.exists && descriptionFilePath.isFile) {
      description = descriptionFilePath.readFileSync({ encoding: 'utf-8' }).trim();
    }

    const pathStructurePath = repoDir.join(id, '.table-dataset', 'meta', 'path-structure.json');
    const pathStructure = PathStructure.fromFile(pathStructurePath);

    const schemaFilePath = repoDir.join(id, '.table-dataset', 'meta', 'schema.json');
    const schema = Schema.fromFile(schemaFilePath);

    const legendDirPath = repoDir.join(id, '.table-dataset', 'meta', 'legend');
    const legendFiles = legendDirPath.readDirectorySync();
    const legends = new Legends();
    for (const legendFile of legendFiles) {
      const legend = Legend.fromFile(legendFile);
      legends.add(legend);
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
   *
   * Caution: Values have been mapped to their respective column names, but they have not been validated.
   * Clients should use `toDecodedFeatures()` to get fully validated features.
   */
  protected discoverRawFeatures() {
    const featureDirPath = this.#datasetPath.join('.table-dataset', 'feature');
    if (!featureDirPath.exists) {
      return []; // a missing folder indicates no features
    }

    const featureFiles = featureDirPath.readDirectorySync({ recursive: true });

    return featureFiles
      .filter((file) => file.isFile)
      .map((file) => {
        const rawFeature = RawFeature.fromFile(file);
        const rawFeatureData = rawFeature.toObject(this.legends, this.schema, this.pathStructure);

        const pathAfterFeatureDir = featureDirPath.relativeTo(file);

        const primaryKeyNames = this.schema.filter((entry) => entry.isPrimary).map((entry) => entry.name);

        const properties = Object.fromEntries([
          ...rawFeatureData.ids.entries(),
          ...rawFeatureData.properties.entries(),
        ]);

        return {
          path: pathAfterFeatureDir,
          legendId: rawFeature.legendId,
          primaryKeys: primaryKeyNames,
          primaryGeometryKey: rawFeatureData.metadata.geometryColumn?.name,
          schema: this.schema.toJsonSchema(),
          properties,
          crs: rawFeatureData.metadata.crs,
        };
      });
  }

  /**
   * Gets each feature inside the feature folder for the dataset in its raw form.
   */
  protected toRawFeatures() {
    const featureDirPath = this.#datasetPath.join('.table-dataset', 'feature');
    if (!featureDirPath.exists) {
      return []; // a missing folder indicates no features
    }

    const featureFiles = featureDirPath.readDirectorySync({ recursive: true });
    return featureFiles.filter((file) => file.isFile).map((file) => RawFeature.fromFile(file));
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

          if (!feature.primaryGeometryKey) {
            return;
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

// ensure that the temporal dates print nicely in the console
(async () => {
  if (process.env.TARGET === 'node') {
    const { inspect } = await import('node:util');
    type InspectOptions = Parameters<typeof inspect>[1];

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
  }
})();
