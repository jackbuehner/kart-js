import z from 'zod';

const dataType = z.literal([
  'boolean',
  'blob',
  'date',
  'float',
  'geometry',
  'integer',
  'interval',
  'numeric',
  'text',
  'time',
  'timestamp',
]);

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

const booleanCol = base.extend({
  dataType: z.literal('boolean'),
});

const blobCol = base.extend({
  dataType: z.literal('blob'),
});

const dateCol = base.extend({
  dataType: z.literal('date'),
});

const integerCol = base.extend({
  dataType: z.literal('integer'),
  /**
   * The size of the integer in bits.
   *
   * @remarks
   * Integers may be 8, 16, 32, or 64 bits.
   */
  size: z.union([z.literal(8), z.literal(16), z.literal(32), z.literal(64)]),
});

const intervalCol = base.extend({
  dataType: z.literal('interval'),
});

const floatCol = base.extend({
  dataType: z.literal('float'),
  /**
   * The size of the float in bits.
   *
   * @remarks
   * Floats may be 32 or 64 bits.
   */
  size: z.union([z.literal(32), z.literal(64)]),
});

const textCol = base.extend({
  dataType: z.literal('text'),
  /**
   * If the column `dataType` is `text`, this specifies the
   * maximum length of the text in characters.
   *
   * If the length is unbounded, the value should be `null` or `undefined`.
   */
  length: z.number().int().nullable().optional(),
});

const numericCol = base.extend({
  dataType: z.literal('numeric'),
  /**
   * The total number of digits.
   */
  precision: z.number().int(),
  /**
   * The number of digits to the right of the decimal point.
   */
  scale: z.number().int(),
});

const timeCol = base.extend({
  dataType: z.literal('time'),
});

const timestampCol = base.extend({
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
});

const geometryCol = base.extend({
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
});

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
