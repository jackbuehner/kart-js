import { Decimal } from 'decimal.js';
import { Temporal } from 'temporal-polyfill';
import { GeoJSONGeometrySchema } from 'zod-geojson';
import { Enumerable } from '../utils/Enumerable.ts';
import { reprojectFeature, type GeometryWithCrs, type KartEnabledFeature } from '../utils/features/index.ts';
import type { CRSs } from './CRS.ts';
import { Legends } from './Legend.ts';
import type { PathStructure } from './PathStructure.ts';
import type { RawFeature } from './RawFeature.ts';
import type { Schema, SchemaEntry, SchemaEntryTypes } from './Schema.ts';

export class Feature {
  readonly schema: Schema;
  readonly legends: Legends;
  readonly crss: CRSs;

  readonly ids: ReturnType<typeof RawFeature.prototype.toObject>['ids'];
  readonly properties: ReturnType<typeof RawFeature.prototype.toObject>['properties'];
  readonly metadata: ReturnType<typeof RawFeature.prototype.toObject>['metadata'];

  private constructor(
    ids: typeof Feature.prototype.ids,
    properties: typeof Feature.prototype.properties,
    metadata: typeof Feature.prototype.metadata,
    schema: typeof Feature.prototype.schema,
    legends: typeof Feature.prototype.legends,
    crss: typeof Feature.prototype.crss
  ) {
    this.ids = ids;
    this.properties = properties;
    this.metadata = metadata;
    this.schema = schema;
    this.legends = legends;
    this.crss = crss;
  }

  static fromRawFeature(
    rawFeature: RawFeature,
    schema: Schema,
    legends: Legends,
    pathStructure: PathStructure,
    crss: CRSs
  ) {
    const rawFeatureObject = rawFeature.toObject(legends, schema, pathStructure, crss);
    return new Feature(
      rawFeatureObject.ids,
      rawFeatureObject.properties,
      rawFeatureObject.metadata,
      schema,
      legends,
      crss
    );
  }

  /**
   * Creates a new Feature instance from a GeoJSON Feature object.
   *
   * The GeoJSON feature must have properties and _kart.ids defined.
   *
   * The properties and ids will be validated against the provided schema.
   *
   * @throws {Error} If the feature does not have properties or _kart.ids defined.
   * @throws {AggregateValidationError} If any of the values fail validation against their expected types in the schema. The error will include details about all validation errors encountered during the validation process.
   */
  static fromGeoJSON(geojsonFeature: KartEnabledFeature<GeometryWithCrs>, schema: Schema, crss: CRSs) {
    if (!geojsonFeature.properties) {
      throw new Error('Feature must have properties to be converted to a RawFeature.');
    }

    if (!geojsonFeature._kart.ids) {
      throw new Error('Feature must have _kart.ids to be converted to a RawFeature.');
    }

    const ids = new Map<string, unknown>(Object.entries(geojsonFeature._kart.ids));
    const properties = new Map<string, unknown>(Object.entries(geojsonFeature.properties));
    properties.set(geojsonFeature._kart.geometryColumn.name, geojsonFeature.geometry);

    const feature = new Feature(
      ids,
      properties,
      {
        geometryColumn: geojsonFeature._kart.geometryColumn,
        crs: geojsonFeature.geometry.crs?.properties.name ?? null,
        droppedKeys: [],
        eid: geojsonFeature._kart.eid,
      },
      schema,
      new Legends([schema.toLegend()]),
      crss
    );

    const result = feature.validate();
    if (!result.ok) {
      throw new AggregateValidationError(result.errors);
    }

    return feature;
  }

  /**
   * Checks whether the feature complies with the given Kart Table Dataset V3 schema.
   *
   * @remarks
   * This function validates the feature's properties against the schema entries,
   * ensuring that each property matches the expected data type and constraints defined in the schema.
   *
   * Whenever a change is made to a feature, a new Feature inistance should be
   * created and this method should be called to ensure that the feature remains
   * compliant with the schema.
   *
   * Every key in the schema will be checked in the feature.
   */
  validate() {
    const errors: Map<string, Error[]> = new Map();

    for (const schemaEntry of this.schema) {
      const key = schemaEntry.name;
      try {
        const result = this.getValue(key);
        if (!result.ok) {
          errors.set(key, result.errors ?? []);
        }
      } catch (error) {
        errors.set(key, [error instanceof Error ? error : new Error(String(error))]);
      }
    }

    if (errors.size > 0) {
      return { ok: false as false, errors };
    }

    return { ok: true as true };
  }

  /**
   * Upgrades the feature to the latest schema and returns
   * it as a structured object of ids (primary keys) and properties (non-primary keys).
   *
   * @throws {Error} If the legend with the specified ID is not found in the class's legends collection.
   * @throws {AggregateValidationError} If any of the values fail validation against their expected types in the schema. The error will include details about all validation errors encountered during the validation process.
   *
   * @returns An object containing the upgraded ids, properties, and metadata about the upgrade process.
   */
  toObject(): ReturnType<typeof RawFeature.prototype.toObject> {
    type ValidatedValue = ReturnType<typeof this.getValue>['data'];

    const validatedIds = new Map<string, ValidatedValue>();
    const validatedProperties = new Map<string, ValidatedValue>();
    const validationErrors = new Map<string, Error[]>();

    for (const key of this.ids.keys()) {
      const validatedValue = this.getValue(key);
      if (validatedValue.ok) {
        validatedIds.set(key, validatedValue.data);
      } else {
        validationErrors.set(key, validatedValue.errors ?? []);
      }
    }

    for (const key of this.properties.keys()) {
      const validatedValue = this.getValue(key);
      if (validatedValue.ok) {
        validatedProperties.set(key, validatedValue.data);
      } else {
        validationErrors.set(key, validatedValue.errors ?? []);
      }
    }

    if (validationErrors.size > 0) {
      throw new AggregateValidationError(validationErrors);
    }

    return {
      ids: validatedIds as ReadonlyMap<string, ValidatedValue>,
      properties: validatedProperties as ReadonlyMap<string, ValidatedValue>,
      metadata: this.metadata,
    };
  }

  /**
   * Converts the feature into a GeoJSON Feature object.
   *
   * This function will upgrade raw feature to the latest schema via `toObject()`
   * before converting it to a GeoJSON Feature.
   *
   * @throws When an error is thrown by `toObject()`.
   */
  toGeoJSON() {
    const { ids, properties, metadata } = this.toObject();

    const geometryColumn = metadata.geometryColumn;
    const geometry = geometryColumn ? (properties.get(geometryColumn.name) as GeoJSON.Geometry) : null;
    if (!geometryColumn || !geometry || !metadata.crs) {
      return null;
    }

    const geometryWithCrs = geometry as GeometryWithCrs;
    geometryWithCrs.crs = { type: 'name', properties: { name: metadata.crs } };

    const featureProperties: Record<string, unknown> = {};
    properties.forEach((value, key) => {
      if (key !== geometryColumn.name) {
        featureProperties[key] = value;
      }
    });

    return reprojectFeature(
      {
        type: 'Feature',
        id: metadata.eid,
        _kart: {
          ids: Object.fromEntries(ids),
          eid: metadata.eid,
          geometryColumn,
        },
        properties: featureProperties,
        geometry: geometryWithCrs,
      } as KartEnabledFeature<GeometryWithCrs>,
      'EPSG:4326',
      this.crss
    ) as KartEnabledFeature<GeometryWithCrs>;
  }

  /**
   * Checks the schema for the given key and returns its data type if it exists.
   *
   * If the key is not found in the schema, it returns undefined.
   */
  getType(key: string): SchemaEntry['dataType'] | undefined {
    const schemaEntry = this.schema.find((entry) => entry.name === key);
    return schemaEntry ? schemaEntry.dataType : undefined;
  }

  /**
   * Attempts to retreive a value for the given key from the raw feature.
   */
  private getKeyValue<T = unknown>(key: string): T | undefined {
    if (this.ids.has(key)) {
      return this.ids.get(key) as unknown as T;
    }

    if (this.properties.has(key)) {
      return this.properties.get(key) as unknown as T;
    }

    return undefined;
  }

  /**
   * Whether the given key is a primary key according to the schema.
   */
  private isPrimaryKey(key: string): boolean {
    return this.schema.find((entry) => entry.name === key)?.isPrimary ?? false;
  }

  /**
   * Gets the value for the given key according to the specified data type.
   *
   * The result indicates the following:
   * - `type`: The data type of the property as defined in the schema.
   * - `isPrimaryKey`: Whether the property is a primary key.
   * - `ok`: Whether the value was successfully retrieved and validated without errors.
   * - `errors`: An array of errors encountered during retrieval or validation, If there are no errors, this will be `undefined`.
   * - `data`: The retrieved and validated value, or `null` if the value is missing. If there were errors retrieving the value, this will be `undefined`.
   *
   * If the found value does not match the expected type but can be coerced, it will be coerced.
   *
   * For types in the schema with additional constraints, this method will return the value
   * even if it violates those constraints. Always check whether `ok` is `true` before using the `data` value.
   * When a constraint is violated, an error will be included in the `errors` array.
   */
  getValue(
    key: string
  ): ValidProperty<
    | Uint8Array
    | boolean
    | Temporal.PlainDate
    | number
    | GeoJSON.Geometry
    | bigint
    | Temporal.Duration
    | Decimal
    | string
    | Temporal.PlainTime
    | Temporal.PlainDateTime
  > {
    const type = this.getType(key);
    if (!type) {
      throw new Error(`Key "${key}" not found in schema.`);
    }

    // prettier-ignore
    switch (type) {
      case 'blob': return this.getBlob(key);
      case 'boolean': return this.getBoolean(key);
      case 'date': return this.getDate(key);
      case 'float': return this.getFloat(key);
      case 'geometry': return this.getGeometry(key);
      case 'integer': return this.getInteger(key);
      case 'interval': return this.getInterval(key);
      case 'numeric': return this.getNumeric(key);
      case 'text': return this.getText(key);
      case 'time': return this.getTime(key);
      case 'timestamp': return this.getTimestamp(key);
    }
  }

  /**
   * Explicitly gets a value as a blob (Uint8Array).
   *
   * ### Supported formats:
   * - Uint8Array
   * - ArrayBuffer
   * - Base64 string that can be decoded into a Uint8Array
   * - Array<number> where every number is from 0 to 255 (inclusive)
   *
   * @see getValue
   */
  getBlob(key: string): ValidProperty<Uint8Array> {
    const type = this.getType(key);
    if (type !== 'blob') {
      throw new TypeMismatchError('blob', type);
    }

    const rawValue = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (rawValue === undefined || rawValue === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    if (rawValue instanceof Uint8Array) {
      return { type, isPrimaryKey, ok: true, data: rawValue };
    }

    if (rawValue instanceof ArrayBuffer) {
      const value = new Uint8Array(rawValue);
      return { type, isPrimaryKey, ok: true, data: value };
    }

    // assume the string is base64 encoded blob data
    if (typeof rawValue === 'string') {
      try {
        const value = Uint8Array.fromBase64(rawValue);
        return { type, isPrimaryKey, ok: true, data: value };
      } catch (error) {
        return {
          type,
          isPrimaryKey,
          ok: false,
          errors: [error instanceof Error ? error : new Error(String(error))],
        };
      }
    }

    // assume numerical arrays with values from 0 to 255 are the Array form of an ArrayBuffer
    if (
      Array.isArray(rawValue) &&
      rawValue.every((item): item is number => typeof item === 'number' && item >= 0 && item <= 255)
    ) {
      const value = new Uint8Array(rawValue);
      return { type, isPrimaryKey, ok: true, data: value };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [
        new InvalidValueError(
          key,
          'Uint8Array, base64-encoded Uint8Array, or Array<number> where every number is from 0 to 255 (inclusive)',
          rawValue
        ),
      ],
    };
  }

  /**
   * Explicitly gets a value as a boolean.
   *
   * ### Supported formats:
   * - boolean
   * - number 0 or 1
   * - string "true" or "false" (case-insensitive)
   *
   * @see getValue
   */
  getBoolean(key: string): ValidProperty<boolean> {
    const type = this.getType(key);
    if (type !== 'boolean') {
      throw new TypeMismatchError('boolean', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isBoolean = typeof value === 'boolean';
    if (isBoolean) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isBooleanNumber = typeof value === 'number' && (value === 0 || value === 1);
    if (isBooleanNumber) {
      return { type, isPrimaryKey, ok: true, data: value === 1 };
    }

    const isBooleanString =
      typeof value === 'string' && (value.toLowerCase() === 'true' || value.toLowerCase() === 'false');
    if (isBooleanString) {
      return { type, isPrimaryKey, ok: true, data: value.toLowerCase() === 'true' };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [
        new InvalidValueError(
          key,
          'boolean, number 0 or 1, or string "true" or "false" (case-insensitive)',
          value
        ),
      ],
    };
  }

  /**
   * Explicitly gets a value as a Temporal.PlainDate.
   *
   * ### Supported formats:
   * - Temporal.PlainDate
   * - ISO 8601 date string
   *
   * @see getValue
   */
  getDate(key: string): ValidProperty<Temporal.PlainDate> {
    const type = this.getType(key);
    if (type !== 'date') {
      throw new TypeMismatchError('date', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isPlainDate = value instanceof Temporal.PlainDate;
    if (isPlainDate) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isPlainDateString = typeof value === 'string' && Temporal.PlainDate.from(value).toString() === value;
    if (isPlainDateString) {
      return { type, isPrimaryKey, ok: true, data: Temporal.PlainDate.from(value) };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [new InvalidValueError(key, 'Temporal.PlainDate or ISO 8601 date string', value)],
    };
  }

  /**
   * Alias of `getNumeric()`.
   * @see getNumeric
   */
  getDecimal(key: string) {
    return this.getNumeric(key);
  }

  /**
   * Alias of `getInterval()`.
   * @see getInterval
   */
  getDuration(key: string) {
    return this.getInterval(key);
  }

  /**
   * Explicitly gets a value as a float (number).
   *
   * ### Supported formats:
   * - finite number
   * - string that can be parsed into a finite number
   *
   * @see getValue
   */
  getFloat(key: string): ValidProperty<number> {
    const type = this.getType(key);
    if (type !== 'float') {
      throw new TypeMismatchError('float', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isFloat = typeof value === 'number' && Number.isFinite(value);
    if (isFloat) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isFloatString = typeof value === 'string' && Number.isFinite(Number(value));
    if (isFloatString) {
      return { type, isPrimaryKey, ok: true, data: Number(value) };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [
        new InvalidValueError(key, 'finite number or string that can be parsed into a finite number', value),
      ],
    };
  }

  /**
   * Explicitly gets a value as a GeoJSON.Geometry.
   *
   * ### Supported formats:
   * - GeoJSON.Geometry object (validated against GeoJSON schema)
   *
   * @see getValue
   */
  getGeometry(key: string): ValidProperty<GeoJSON.Geometry> {
    const type = this.getType(key);
    if (type !== 'geometry') {
      throw new TypeMismatchError('geometry', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const result = GeoJSONGeometrySchema.safeParse(value);
    if (result.success) {
      return { type, isPrimaryKey, ok: true, data: result.data };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [result.error],
    };
  }

  /**
   * Explicitly gets a value as an integer (bigint).
   *
   * ### Supported formats:
   * - bigint
   * - integer number
   * - string that can be parsed into a bigint (with or without trailing 'n')
   *
   * @see getValue
   */
  getInteger(key: string): ValidProperty<bigint> {
    const type = this.getType(key);
    if (type !== 'integer') {
      throw new TypeMismatchError('integer', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isBigInt = typeof value === 'bigint';
    if (isBigInt) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isIntegerNumber = typeof value === 'number' && Number.isInteger(value);
    if (isIntegerNumber) {
      return { type, isPrimaryKey, ok: true, data: BigInt(value) };
    }

    const isBigIntString = typeof value === 'string' && value.trim().match(/^-?\d+n$/);
    if (isBigIntString) {
      return { type, isPrimaryKey, ok: true, data: BigInt(value.trim().slice(0, -1)) };
    }

    const isIntegerString = typeof value === 'string' && value.trim().match(/^-?\d+$/);
    if (isIntegerString) {
      return { type, isPrimaryKey, ok: true, data: BigInt(value.trim()) };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [
        new InvalidValueError(key, 'bigint, integer number, or string that can be parsed into a bigint', value),
      ],
    };
  }

  /**
   * Explicitly gets a value as a Temporal.Duration.
   *
   * ### Supported formats:
   * - Temporal.Duration
   * - ISO 8601 duration string
   *
   * @see getValue
   */
  getInterval(key: string): ValidProperty<Temporal.Duration> {
    const type = this.getType(key);
    if (type !== 'interval') {
      throw new TypeMismatchError('interval', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isDuration = value instanceof Temporal.Duration;
    if (isDuration) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isDurationString = typeof value === 'string' && Temporal.Duration.from(value).toString() === value;
    if (isDurationString) {
      return { type, isPrimaryKey, ok: true, data: Temporal.Duration.from(value) };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [new InvalidValueError(key, 'Temporal.Duration or ISO 8601 duration', value)],
    };
  }

  /**
   * Explicitly gets a value as a Decimal.
   *
   * ### Supported formats:
   * - Decimal
   * - string that can be parsed into a Decimal
   *
   * @see getValue
   */
  getNumeric(key: string): ValidProperty<Decimal> {
    const type = this.getType(key);
    if (type !== 'numeric') {
      throw new TypeMismatchError('numeric', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);
    const schemaEntry = this.schema.find((entry) => entry.name === key) as SchemaEntryTypes.Numeric;

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const errors: Error[] = [];
    const checkDecimal = (decimal: Decimal) => {
      if (decimal.precision() > schemaEntry.precision) {
        errors.push(
          new InvalidValueError(
            key,
            `Decimal with precision (total number of digits) less than or equal to ${schemaEntry.precision}`,
            value.toString()
          )
        );
      }

      if (decimal.decimalPlaces() > schemaEntry.scale) {
        errors.push(
          new InvalidValueError(
            key,
            `Decimal with scale (total number of digits to the right of the decimal point) less than or equal to ${schemaEntry.scale}`,
            value.toString()
          )
        );
      }
    };

    const isDecimal = value instanceof Decimal;
    if (isDecimal) {
      checkDecimal(value);
      return { type, isPrimaryKey, ok: true, data: value, errors: errors.length > 0 ? errors : undefined };
    }

    const isDecimalString = typeof value === 'string' && new Decimal(value).toString() === value;
    if (isDecimalString) {
      const decimal = new Decimal(value);
      checkDecimal(decimal);
      return { type, isPrimaryKey, ok: true, data: decimal, errors: errors.length > 0 ? errors : undefined };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [
        new InvalidValueError(key, 'Decimal or string that can be parsed into a Decimal', value),
        ...errors,
      ],
    };
  }

  /**
   * Explicitly gets a value as a string.
   *
   * ### Supported formats:
   * - string
   *
   * @see getValue
   */
  getText(key: string): ValidProperty<string> {
    const type = this.getType(key);
    if (type !== 'text') {
      throw new TypeMismatchError('text', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isString = typeof value === 'string';
    if (!isString) {
      return {
        type,
        isPrimaryKey,
        ok: false,
        errors: [new InvalidValueError(key, 'string', value)],
      };
    }

    const schemaEntry = this.schema.find((entry) => entry.name === key) as SchemaEntryTypes.Text;
    const isUnderMaxLength =
      schemaEntry.length === undefined || value.length <= (schemaEntry.length ?? Infinity);
    if (!isUnderMaxLength) {
      return {
        type,
        isPrimaryKey,
        ok: false,
        errors: [
          new InvalidValueError(key, `string with length less than or equal to ${schemaEntry.length}`, value),
        ],
      };
    }

    return { type, isPrimaryKey, ok: true, data: value };
  }

  /**
   * Explicitly gets a value as a Temporal.PlainTime.
   *
   * ### Supported formats:
   * - Temporal.PlainTime
   * - HH:mm:ss.fractional time string
   *
   * @see getValue
   */
  getTime(key: string): ValidProperty<Temporal.PlainTime> {
    const type = this.getType(key);
    if (type !== 'time') {
      throw new TypeMismatchError('time', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isPlainTime = value instanceof Temporal.PlainTime;
    if (isPlainTime) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isPlainTimeString = typeof value === 'string' && Temporal.PlainTime.from(value).toString() === value;
    if (isPlainTimeString) {
      return { type, isPrimaryKey, ok: true, data: Temporal.PlainTime.from(value) };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [new InvalidValueError(key, 'Temporal.PlainTime or HH:mm:ss.fractional time', value)],
    };
  }

  /**
   * Explicitly gets a value as a Temporal.PlainDateTime.
   *
   * ### Supported formats:
   * - Temporal.PlainDateTime
   * - ISO 8601 timestamp string without a timezone
   *
   * @see getValue
   */
  getTimestamp(key: string): ValidProperty<Temporal.PlainDateTime> {
    const type = this.getType(key);
    if (type !== 'timestamp') {
      throw new TypeMismatchError('timestamp', type);
    }

    const value = this.getKeyValue(key);
    const isPrimaryKey = this.isPrimaryKey(key);

    if (value === undefined || value === null) {
      return { type, isPrimaryKey, ok: true, data: null };
    }

    const isPlainDateTime = value instanceof Temporal.PlainDateTime;
    if (isPlainDateTime) {
      return { type, isPrimaryKey, ok: true, data: value };
    }

    const isPlainDateTimeString =
      typeof value === 'string' && Temporal.PlainDateTime.from(value).toString() === value;
    if (isPlainDateTimeString) {
      return { type, isPrimaryKey, ok: true, data: Temporal.PlainDateTime.from(value) };
    }

    return {
      type,
      isPrimaryKey,
      ok: false,
      errors: [
        new InvalidValueError(
          key,
          'Temporal.PlainDateTime or ISO 8601 timestamp string without a timezone',
          value
        ),
      ],
    };
  }
}

export class Features extends Enumerable<Feature> {}

class TypeMismatchError extends Error {
  constructor(expectedType: string, actualType?: string) {
    super(`Type mismatch: expected ${expectedType}, got ${actualType ?? 'undefined'}`);
    this.name = 'TypeMismatchError';
  }
}

class InvalidValueError extends Error {
  constructor(key: string, expectedType: string, actualValue: unknown) {
    super(
      `Invalid value for key "${key}": expected type ${expectedType}, got value ${JSON.stringify(actualValue)}`
    );
    this.name = 'InvalidValueError';
  }
}

export class AggregateValidationError extends Error {
  errors: Map<string, Error[]>;

  constructor(errors: Map<string, Error[]>) {
    const errorMessages = Array.from(errors.entries()).map(
      ([key, errs]) => `Key "${key}":\n  ${errs.map((err) => err.message).join('\n  ')}`
    );
    super(`Multiple validation errors:\n${errorMessages.join('\n')}`);
    this.name = 'AggregateValidationError';
    this.errors = errors;
  }
}

interface ValidProperty<T> {
  type: SchemaEntry['dataType'];
  isPrimaryKey: boolean;
  ok: boolean;
  errors?: Error[];
  data?: T | null;
}
