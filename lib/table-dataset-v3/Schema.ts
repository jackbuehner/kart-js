import { Decimal } from 'decimal.js';
import { Temporal } from 'temporal-polyfill';
import z from 'zod';
import type { Path } from '../utils/Path.ts';
import { FileNotFoundError, InvalidFileContentsError } from '../utils/errors.ts';
import { Legend } from './Legend.ts';

type SchemaEntry = z.infer<typeof schemaEntrySchema>;

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
   * Validates a feature's data against the schema entries.
   *
   * The input must be a single object that includes the primary and non-primary key properties
   *
   * @param data - The feature data to validate.
   *
   * @returns An object containing the validated data and any validation errors.
   */
  validateFeature(data: Record<string, unknown>) {
    const errors: string[] = [];
    const checkedProperties = new Set<string>();
    const decoded: Record<string, unknown> = {};

    const getValue = (key: string) => {
      return data[key];
    };

    for (const entry of this.schemaEntries) {
      const value = getValue(entry.name);
      checkedProperties.add(entry.name);

      const getType = (val: unknown) => {
        if (val === null) return 'null';
        if (val === undefined) return 'undefined';
        if (typeof val === 'object') {
          if (val.constructor && val.constructor.name) {
            return val.constructor.name;
          }
          return Object.prototype.toString.call(val).slice(8, -1);
        }
        return typeof val;
      };

      // BLOB
      if (entry.dataType === 'blob') {
        // DECODE
        if (value instanceof Uint8Array) {
          decoded[entry.name] = value;
        } else if (typeof value === 'string') {
          const byteArray = new Uint8Array(value.length);
          for (let i = 0; i < value.length; i++) {
            byteArray[i] = value.charCodeAt(i);
          }
          decoded[entry.name] = byteArray;
        } else if (Array.isArray(value) && value.every((v) => typeof v === 'number' && v >= 0 && v <= 255)) {
          decoded[entry.name] = new Uint8Array(value);
        } else {
          decoded[entry.name] = null;
        }

        // CHECK
        // blobs should be Uint8Array
        const validator = z.instanceof(Uint8Array).nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a blob (Uint8Array) or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }

        continue;
      }

      // BOOLEAN
      if (entry.dataType === 'boolean') {
        // DECODE
        if (
          typeof value === 'boolean' ||
          (typeof value === 'number' && (value === 0 || value === 1)) ||
          (typeof value === 'string' && (value.toLowerCase() === 'true' || value.toLowerCase() === 'false'))
        ) {
          decoded[entry.name] = Boolean(value);
        } else {
          decoded[entry.name] = null;
        }

        // CHECK
        const validator = z.boolean().nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a boolean or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }

        continue;
      }

      // DATE
      if (entry.dataType === 'date') {
        // DECODE
        try {
          decoded[entry.name] = Temporal.PlainDate.from(value as string);
        } catch {
          decoded[entry.name] = null;
        }

        // CHECK
        const validator = z.instanceof(Temporal.PlainDate).nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a Temporal.PlainDate or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }

        continue;
      }

      // FLOAT
      if (entry.dataType === 'float') {
        // DECODE
        let float: number | null = Number(value);
        if (isNaN(float)) {
          float = null;
        }
        decoded[entry.name] = float;

        // CHECK
        const validator = z.number().nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a float (number) or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }

        continue;
      }

      // GEOMETRY
      if (entry.dataType === 'geometry') {
        // CHECK
        // do not deeply check validity, but check that it matches the shape of GeoJSON.Geometry
        const validator = z
          .object({
            type: z.string(),
            coordinates: z.any(),
          })
          .nullable();
        const result = z.safeParse(validator, value);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a GeoJSON Geometry object or null. Got: ${getType(value)}`
          );
        }

        // DECODE
        if (result.success) {
          decoded[entry.name] = value;
        } else {
          decoded[entry.name] = null;
        }

        continue;
      }

      // INTEGER
      if (entry.dataType === 'integer') {
        // DECODE
        // since 64-bit integers cannot fit inside a number, we use bigint for integer types
        try {
          let intValue: bigint | null = null;
          if (typeof value === 'bigint') {
            intValue = value;
          } else if (typeof value === 'number') {
            intValue = BigInt(value);
          } else if (typeof value === 'string') {
            intValue = BigInt(value);
          } else {
            intValue = null;
          }
          decoded[entry.name] = intValue;
        } catch {
          decoded[entry.name] = null;
        }

        // CHECK
        const validator = z.bigint().nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a bigint or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }
        continue;
      }

      // INTERVAL
      if (entry.dataType === 'interval') {
        // DECODE
        try {
          Temporal.Duration.from(value as string);
        } catch {
          decoded[entry.name] = null;
          continue;
        }

        // CHECK
        const validator = z.instanceof(Temporal.Duration).nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a Temporal.Duration or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }
        continue;
      }

      // NUMERIC
      if (entry.dataType === 'numeric') {
        // DECODE
        try {
          const decimal = new Decimal(value as string);
          decimal.toPrecision(entry.precision);
          decimal.toDecimalPlaces(entry.scale);
          decoded[entry.name] = decimal;
        } catch {
          decoded[entry.name] = null;
        }

        // CHECK
        const validator = z.instanceof(Decimal).nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a Decimal or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }
        continue;
      }

      // TEXT
      if (entry.dataType === 'text') {
        // DECODE
        if (typeof value === 'string') {
          decoded[entry.name] = value;
        } else if (value != null) {
          decoded[entry.name] = String(value);
        } else {
          decoded[entry.name] = null;
        }

        // trim to maxLength if necessary
        if (
          entry.length &&
          decoded[entry.name] &&
          typeof decoded[entry.name] === 'string' &&
          (decoded[entry.name] as string).length > entry.length
        ) {
          decoded[entry.name] = (decoded[entry.name] as string).slice(0, entry.length);
        }

        // CHECK
        const validator = entry.length ? z.string().max(entry.length).nullable() : z.string().nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          if (result.error.issues.some((issue) => issue.code === 'too_big')) {
            errors.push(
              `Property "${entry.name}" exceeds maximum length of ${entry.length}. Got length: ${(value as string).length}`
            );
            continue;
          }

          errors.push(
            `Property "${entry.name}" is expected to be a string or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }
        continue;
      }

      // TIME
      if (entry.dataType === 'time') {
        // DECODE
        try {
          decoded[entry.name] = Temporal.PlainTime.from(value as string);
        } catch {
          decoded[entry.name] = null;
        }

        // CHECK
        const validator = z.instanceof(Temporal.PlainTime).nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a Temporal.PlainTime or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }
        continue;
      }

      // TIMESTAMP
      if (entry.dataType === 'timestamp') {
        // DECODE
        try {
          decoded[entry.name] = Temporal.PlainDateTime.from(value as string);
        } catch {
          decoded[entry.name] = null;
        }

        // CHECK
        const validator = z.instanceof(Temporal.PlainDateTime).nullable();
        const result = z.safeParse(validator, decoded[entry.name]);
        if (!result.success) {
          errors.push(
            `Property "${entry.name}" is expected to be a Temporal.PlainDateTime or null. Got: ${getType(decoded[entry.name] ?? value)}`
          );
        }
        continue;
      }
    }

    return { data: decoded, errors };
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
     * If the CRS is unknown, the value should be `null`.
     */
    geometryCrs: geometryCrs.optional(),
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
