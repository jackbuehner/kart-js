import z from 'zod';
import type { Path } from '../utils/Path.ts';
import { FileNotFoundError, InvalidFileContentsError } from '../utils/errors.ts';
import type { CRSs } from './CRS.ts';
import { Legend } from './Legend.ts';

export type SchemaEntry = z.infer<typeof schemaEntrySchema>;

/**
 * A Schema defines the structure of a table dataset, including the columns and their data types.
 *
 * @see https://docs.kartproject.org/en/latest/pages/development/table_v3.html
 */
export class Schema {
  private schemaEntries: SchemaEntry[];

  constructor(schemaEntries: SchemaEntry[]) {
    this.schemaEntries = schemaEntries;
  }

  /**
   * Creates a new Legend instance based on the schema entries in this Schema.
   */
  toLegend() {
    const primaryKeyIdsMap = new Map<number, string>();
    const nonPrimaryKeyIds: string[] = [];

    for (const entry of this.schemaEntries) {
      if (entry.primaryKeyIndex !== undefined && entry.primaryKeyIndex !== null && entry.primaryKeyIndex >= 0) {
        // preserve primary key index because the order must match in the legend
        primaryKeyIdsMap.set(entry.primaryKeyIndex, entry.id);
      } else {
        // non-primary keys are kept in the same order as they appear in the schema entries
        nonPrimaryKeyIds.push(entry.id);
      }
    }

    const primaryKeyIds = Array.from(primaryKeyIdsMap.values());
    return new Legend(primaryKeyIds, nonPrimaryKeyIds);
  }

  /**
   * The ordered names of the primary key columns in the schema.
   *
   * To get the ordered IDs of the primary key columns, use `Schema.toLegend().primaryKeyIds` instead.
   */
  get primaryKeyNames() {
    return this.toLegend().primaryKeyIds.map((id) => {
      const schemaEntry = this.schemaEntries.find((entry) => entry.id === id);
      if (!schemaEntry) {
        throw new Error(`Could not find schema entry for primary key id: ${id}`);
      }
      return schemaEntry.name;
    });
  }

  /**
   * The ordered names of the non-primary key columns in the schema.
   *
   * To get the ordered IDs of the non-primary key columns, use `Schema.toLegend().nonPrimaryKeyIds` instead.
   */
  get nonPrimaryKeyNames() {
    return this.toLegend().nonPrimaryKeyIds.map((id) => {
      const schemaEntry = this.schemaEntries.find((entry) => entry.id === id);
      if (!schemaEntry) {
        throw new Error(`Could not find schema entry for non-primary key id: ${id}`);
      }
      return schemaEntry.name;
    });
  }

  /**
   * Gets the geometry column metadata for the feature based on the schema entries in this Schema.
   * The metadata includes the geometry column name and the CRS for the geometry column.
   *
   * The geometry column used for the feature is the first geometry column in the schema.
   * See https://github.com/koordinates/kart/blob/eae35e1d06273d9cd2638cefd5fdc50250971aa4/kart/sqlalchemy/adapter/gpkg.py#L269-L289
   */
  getFeatureGeometryMetadata(crss: CRSs): FeatureGeometryMetadata {
    const schemaEntry = this.schemaEntries.find((entry) => entry.dataType === 'geometry');

    const crs = (() => {
      if (!schemaEntry) {
        return null;
      }

      if (!schemaEntry.geometryCrs) {
        return 'EPSG:4326';
      }

      return crss.find((crs) => crs.identifier === schemaEntry.geometryCrs)?.identifier ?? null;
    })();

    if (!schemaEntry) {
      return { geometryColumn: null, crs };
    }

    return { geometryColumn: { id: schemaEntry.id, name: schemaEntry.name }, crs };
  }

  /**
   * Converts the Schema instance to a JSON Schema representation.
   *
   * This can be used for interoperability with systems that utilize JSON Schema.
   * Whenever possible, instead of manual validation with the JSON Schema,
   * validate data directly against the Schema instance using `Schema.validate()`.
   */
  toJsonSchema() {
    return {
      $schema: 'https://json-schema.org/draft/2020-12/schema' as const,
      type: 'object' as const,
      properties: Object.fromEntries(
        this.toLegend()
          .columnIds.map(({ columnId }): [string, JsonSchema.DataTypes] | undefined => {
            const columnSchema = this.schemaEntries.find(({ id }) => id === columnId);
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
  }

  /**
   * Creates a Schema instance from a schema file at the given path.
   *
   * @throws {FileNotFoundError} If the file does not exist at the specified path.
   * @throws {import('../utils/errors.ts').FileReadError} If the file cannot be read.
   * @throws {InvalidFileContentsError} If the file is empty of cannot be parsed as JSON.
   * @throws {z.ZodError} If the decoded contents do not match the expected array structure.
   *
   * @param filePath - The path to the schema file.
   */
  static fromFile(filePath: Path) {
    if (!filePath.exists) {
      throw new FileNotFoundError(`Legend file does not exist at path: ${filePath}`);
    }

    const schemaRaw = filePath.readFileSync({ encoding: 'utf-8' });
    if (!schemaRaw) {
      throw new InvalidFileContentsError(`Schema file at path ${filePath} is empty`);
    }

    let schemaJson: unknown;
    try {
      schemaJson = JSON.parse(schemaRaw);
    } catch (error) {
      throw new InvalidFileContentsError(
        `Schema file at path ${filePath} contains invalid JSON: ${(error as Error).message}`
      );
    }

    const schemaEntries = z.array(schemaEntrySchema).parse(schemaJson);
    return new Schema(schemaEntries);
  }

  map(fn: (entry: SchemaEntry) => SchemaEntry) {
    return this.schemaEntries.map(fn);
  }

  filter<S extends SchemaEntry>(fn: (entry: SchemaEntry) => entry is S): S[];
  filter(fn: (entry: SchemaEntry) => boolean): SchemaEntry[];
  filter(fn: (entry: SchemaEntry) => boolean) {
    return this.schemaEntries.filter(fn);
  }

  find<S extends SchemaEntry>(fn: (entry: SchemaEntry) => entry is S): S | undefined;
  find(fn: (entry: SchemaEntry) => boolean): SchemaEntry | undefined;
  find(fn: (entry: SchemaEntry) => boolean) {
    return this.schemaEntries.find(fn);
  }

  forEach(fn: (entry: SchemaEntry) => void) {
    return this.schemaEntries.forEach(fn);
  }

  *[Symbol.iterator]() {
    for (const entry of this.schemaEntries) {
      yield entry;
    }
  }

  toArray() {
    return this.schemaEntries;
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

export namespace JsonSchema {
  export type DataTypes =
    | BlobType
    | BooleanType
    | DateType
    | FloatType
    | GeometryType
    | IntegerType
    | IntervalType
    | NumericType
    | TextType
    | TimeType
    | TimestampType;

  interface BlobType {
    type: 'array';
    items: {
      type: 'integer';
      minimum: 0;
      maximum: 255;
    };
    format: 'bytes';
  }

  interface BooleanType {
    type: 'boolean';
  }

  interface DateType {
    type: 'string';
    format: 'date';
  }

  interface FloatType {
    type: 'number';
    minimum: number;
    maximum: number;
  }

  interface GeometryType {
    type: 'object';
    $ref: 'https://geojson.org/schema/Geometry.json';
  }

  interface IntegerType {
    type: 'integer';
    minimum: bigint;
    maximum: bigint;
  }

  interface IntervalType {
    type: 'string';
    pattern: '^P(\\d+Y)?(\\d+M)?(\\d+D)?(T(\\d+H)?(\\d+M)?(\\d+(\\.\\d+)?S)?)?$';
    format: 'duration';
  }

  interface NumericType {
    type: 'string';
    pattern: `^-?\\d{1,${number}}(\\.\\d{1,${number}})?$`;
    format: 'decimal';
    'x-precision': number;
    'x-scale': number;
  }

  interface TextType {
    type: 'string';
    maxLength?: number;
  }

  interface TimeType {
    type: 'string';
    format: 'time';
  }

  interface TimestampType {
    type: 'string';
    format: 'date-time';
  }
}

const wktGeometryObjectTypes = [
  'POINT',
  'MULTIPOINT',
  'LINESTRING',
  'MULTILINESTRING',
  'POLYGON',
  'MULTIPOLYGON',
  'TRIANGLE',
  'POLYHEDRALSURFACE',
  'TIN',
  'GEOMETRYCOLLECTION',
];
const wktGeometrySuffixes = ['', 'Z', 'M', 'ZM'];
const allPossibleGeometryTypes = wktGeometryObjectTypes.flatMap((type) =>
  wktGeometrySuffixes.map((suffix) => `${type} ${suffix}`.trim())
);
const geometryType = z.literal(allPossibleGeometryTypes);

const geometryCrs = z.templateLiteral([z.string(), ':', z.number()]).nullable();

const base = z.object({
  /**
   * The unique identifier for the column. It has no specific
   * meaning, but it should never change over the lifetime
   * of the column.
   */
  id: z.string(),
  /**
   * The name of the column as it would appear in a table.
   * Use this name in `SELECT` statements.
   */
  name: z.string(),
  /**
   * This controls whether the column is a primary key.
   *
   * If the value is null or undefined or negative, this column
   * is not a primary key.
   *
   * If the value is a positive integer, the column is a primary
   * key. The first primary key column should have index 0, the
   * second 1, and so on.
   */
  primaryKeyIndex: z.number().int().optional().nullable(),
  /**
   * The type of data stored in this column.
   *
   * - `boolean`: true/false values.
   * - `blob`: stores a string of bytes.
   * - `date`: stores a year, month, and day. It must be ISO 8601 format for GeoPackage, but other working copies may read and write other formats.
   * - `float`: stores a floating point number using a fixed number of bits.
   * - `geometry`: stored well-known text (WKT) representations of geometries.
   * - `integer`: stores integer numbers using a fixed number of bits.
   * - `interval`: stores a time interval as a number of years, months, data, hours, minutes, and seconds
   * - `numeric`: stores a decimal number with a fixed number of digits and precision.
   * - `text`: stores a string of text.
   * - `time`: stores a time of day as hour, minutes, and second.
   * - `timestamp`: stores a date and time without a timezone.
   */
  dataType: z.unknown(),
});

const withIsPrimary = <T extends z.ZodRawShape>(schema: z.ZodObject<T>) =>
  schema
    .extend({
      /**
       * Whether this column is a primary key. A column is a primary key
       * when `primaryKeyIndex` is a non-negative integer.
       */
      isPrimary: z.boolean().nullish(),
    })
    .transform((data) => {
      return {
        ...data,
        isPrimary:
          'primaryKeyIndex' in data && typeof data.primaryKeyIndex === 'number' && data.primaryKeyIndex >= 0,
      };
    });

const booleanCol = withIsPrimary(
  base.extend({
    dataType: z.literal('boolean'),
  })
);

const blobCol = withIsPrimary(
  base.extend({
    dataType: z.literal('blob'),
  })
);

const dateCol = withIsPrimary(
  base.extend({
    dataType: z.literal('date'),
  })
);

const integerCol = withIsPrimary(
  base.extend({
    dataType: z.literal('integer'),
    /**
     * The size of the integer in bits.
     *
     * @remarks
     * Integers may be 8, 16, 32, or 64 bits.
     */
    size: z.union([z.literal(8), z.literal(16), z.literal(32), z.literal(64)]),
  })
);

const intervalCol = withIsPrimary(
  base.extend({
    dataType: z.literal('interval'),
  })
);

const floatCol = withIsPrimary(
  base.extend({
    dataType: z.literal('float'),
    /**
     * The size of the float in bits.
     *
     * @remarks
     * Floats may be 32 or 64 bits.
     */
    size: z.union([z.literal(32), z.literal(64)]),
  })
);

const textCol = withIsPrimary(
  base.extend({
    dataType: z.literal('text'),
    /**
     * If the column `dataType` is `text`, this specifies the
     * maximum length of the text in characters.
     *
     * If the length is unbounded, the value should be `null` or `undefined`.
     */
    length: z.number().int().nullable().optional(),
  })
);

const numericCol = withIsPrimary(
  base.extend({
    dataType: z.literal('numeric'),
    /**
     * The total number of digits.
     */
    precision: z.number().int(),
    /**
     * The number of digits to the right of the decimal point.
     */
    scale: z.number().int(),
  })
);

const timeCol = withIsPrimary(
  base.extend({
    dataType: z.literal('time'),
  })
);

const timestampCol = withIsPrimary(
  base.extend({
    dataType: z.literal('timestamp'),
    /**
     * The timezone of the timestamp.
     *
     * @remarks
     * The only supported value is `UTC`. For other cases, the
     * value should be `null`, and clients are responsible
     * correctly interpreting timestamps without timezones.
     */
    timezone: z.literal('UTC').nullable(),
  })
);

const geometryCol = withIsPrimary(
  base.extend({
    dataType: z.literal('geometry'),
    /**
     * The geometry type in WKT format.
     */
    geometryType,
    /**
     * If the column `dataType` is `geometry`, this specifies
     * the coordinate reference system (CRS) used for the
     * geometries.
     *
     * In most cases, this will be an EPSG code in the format `EPSG:1234`.
     * However, technically any string is allowed.
     * The dataset's /meta/crs folder MUST contain a .wkt file with the
     * same name as the CRS identifier (e.g. `EPSG_1234.wkt`) that
     * contains the WKT definition of the CRS.
     *
     * If the CRS is unknown, the value should be `null`.
     */
    geometryCrs: z.string().optional(),
  })
);

export const schemaEntrySchema = z.discriminatedUnion('dataType', [
  booleanCol,
  blobCol,
  dateCol,
  integerCol,
  intervalCol,
  floatCol,
  textCol,
  numericCol,
  timeCol,
  timestampCol,
  geometryCol,
]);

export namespace SchemaEntryTypes {
  export type Boolean = z.infer<typeof booleanCol>;
  export type Blob = z.infer<typeof blobCol>;
  export type Date = z.infer<typeof dateCol>;
  export type Integer = z.infer<typeof integerCol>;
  export type Interval = z.infer<typeof intervalCol>;
  export type Float = z.infer<typeof floatCol>;
  export type Text = z.infer<typeof textCol>;
  export type Numeric = z.infer<typeof numericCol>;
  export type Time = z.infer<typeof timeCol>;
  export type Timestamp = z.infer<typeof timestampCol>;
  export type Geometry = z.infer<typeof geometryCol>;
}

interface FeatureGeometryMetadata {
  /**
   * The primary geometry column used for the feature.
   *
   * If there is no geometry column in the schema, this will be null.
   */
  geometryColumn: {
    /**
     * The ID of the geometry column used for the feature.
     * The ID never changes over the lifetime of the column, even if the column name changes.
     *
     * When working with geojson, use the `name` property instead.
     */
    id: string;
    /**
     * The name of the geometry column used for the feature.
     * The name may change over the lifetime of the column, but it is the label that should be used
     * when working with the contents of a table row/feature.
     */
    name: string;
  } | null;
  /**
   * The Coordinate Reference System (CRS) of the feature's geometry.
   * - Any string value is allowed in this field. It must have a corresponding CRS
   *     .wkt file in the dataset's /meta/crs folder that defines defines the WKT
   *     for the CRS.
   * - If the schema does not have a geometry column, this will be null.
   * - If the schema's primary geometry column does not specify a CRS, this will be EPSG:4326.
   * - If the schema's primary geometry column specifies a CRS that cannot be found, this will be null.
   */
  crs: string | null;
}
