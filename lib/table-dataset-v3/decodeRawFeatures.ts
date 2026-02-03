import { Decimal } from 'decimal.js';
import { Temporal } from 'temporal-polyfill';
import type { TableDatasetV3 } from './TableDatasetV3.ts';

/**
 * Takes the raw features discovered from `TableDatasetV3.discoverRawFeatures()`
 * and decodes them into validated JavaScript types based on the dataset schema.
 */
export function decodeRawFeatures(
  data: ReturnType<InstanceType<typeof TableDatasetV3>['discoverRawFeatures']>
) {
  return data.map((raw) => {
    const decoded: Record<string, unknown> = {};

    for (const [key, value] of Object.entries(raw.properties)) {
      const schema = raw.schema.properties[key];
      if (!schema) continue;

      // BLOB
      if (schema.type === 'array' && schema.format === 'bytes') {
        // ensure value is a Uint8Array
        if (value instanceof Uint8Array) {
          decoded[key] = value;
        } else if (typeof value === 'string') {
          const byteArray = new Uint8Array(value.length);
          for (let i = 0; i < value.length; i++) {
            byteArray[i] = value.charCodeAt(i);
          }
          decoded[key] = byteArray;
        } else if (Array.isArray(value) && value.every((v) => typeof v === 'number' && v >= 0 && v <= 255)) {
          decoded[key] = new Uint8Array(value);
        } else {
          decoded[key] = null;
        }
        continue;
      }

      // BOOLEAN
      if (schema.type === 'boolean') {
        decoded[key] = Boolean(value);
        continue;
      }

      // DATE
      if (schema.type === 'string' && 'format' in schema && schema.format === 'date') {
        try {
          decoded[key] = Temporal.PlainDate.from(value as string);
        } catch {
          decoded[key] = null;
        }
        continue;
      }

      // FLOAT
      if (schema.type === 'number') {
        let float: number | null = Number(value);
        if (isNaN(float) || float < schema.minimum || float > schema.maximum) {
          float = null;
        }

        decoded[key] = float;
        continue;
      }

      // GEOMETRY
      if (
        schema.type === 'object' &&
        '$ref' in schema &&
        schema.$ref === 'https://geojson.org/schema/Geometry.json'
      ) {
        // Loosely check that the value is a GeoJSON geometry object.
        // The msgpack parser should have converted WKT to GeoJSON already.
        if (value && typeof value === 'object' && 'type' in value && 'coordinates' in value) {
          decoded[key] = value;
        } else {
          decoded[key] = null;
        }
        continue;
      }

      // INTEGER
      if (schema.type === 'integer') {
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

          // ensure it fits within the minimum and maximum
          if (intValue !== null && (intValue < schema.minimum || intValue > schema.maximum)) {
            intValue = null;
          }

          decoded[key] = intValue;
        } catch {
          decoded[key] = null;
        }
        continue;
      }

      // INTERVAL
      if (schema.type === 'string' && 'format' in schema && schema.format === 'duration') {
        try {
          Temporal.Duration.from(value as string);
        } catch {
          decoded[key] = null;
          continue;
        }
        continue;
      }

      // TIME
      if (schema.type === 'string' && 'format' in schema && schema.format === 'time') {
        try {
          decoded[key] = Temporal.PlainTime.from(value as string);
        } catch {
          decoded[key] = null;
        }
        continue;
      }

      // TIMESTAMP
      if (schema.type === 'string' && 'format' in schema && schema.format === 'date-time') {
        try {
          decoded[key] = Temporal.PlainDateTime.from(value as string);
        } catch {
          decoded[key] = null;
        }
        continue;
      }

      // NUMERIC
      if (schema.type === 'string' && 'format' in schema && schema.format === 'decimal') {
        try {
          const decimal = new Decimal(value as string);
          decimal.toPrecision(schema['x-precision']);
          decimal.toDecimalPlaces(schema['x-scale']);
          decoded[key] = decimal;
        } catch {
          decoded[key] = null;
        }
        continue;
      }

      // TEXT
      if (schema.type === 'string') {
        if (typeof value === 'string') {
          decoded[key] = value;
        } else if (value != null) {
          decoded[key] = String(value);
        } else {
          decoded[key] = null;
        }

        // trim to maxLength if necessary
        if (
          'maxLength' in schema &&
          typeof schema.maxLength === 'number' &&
          typeof decoded[key] === 'string' &&
          decoded[key].length > schema.maxLength
        ) {
          decoded[key] = decoded[key].slice(0, schema.maxLength);
        }

        continue;
      }
    }

    return {
      ...raw,
      properties: decoded,
    };
  });
}
