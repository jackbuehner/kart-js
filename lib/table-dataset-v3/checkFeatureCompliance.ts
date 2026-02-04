import { Decimal } from 'decimal.js';
import { Temporal } from 'temporal-polyfill';
import z from 'zod';
import type { Schema } from './Schema.ts';

/**
 * Checks whether a GeoJSON feature complies with the given Kart Table Dataset V3 schema.
 *
 * @remarks
 * This function validates the feature's properties against the schema entries,
 * ensuring that each property matches the expected data type and constraints defined in the schema.
 *
 * Whenever a change is made in the working feature collection, this function should be
 * called to ensure that the feature remains compliant with the schema.
 */
export function checkFeatureCompliance(feature: GeoJSON.Feature, schema: Schema) {
  const errors: string[] = [];
  const checkedProperties = new Set<string>();

  if (!feature.properties) {
    return { valid: true, errors };
  }

  const primaryKeys = schema
    .filter(({ primaryKeyIndex }) => typeof primaryKeyIndex === 'number' && primaryKeyIndex >= 0)
    .map(({ name }) => name);
  const geometryColumns = schema.filter(({ dataType }) => dataType === 'geometry').map(({ name }) => name);
  const primaryGeometryKey = geometryColumns.includes('geometry')
    ? 'geometry'
    : geometryColumns.includes('geom')
      ? 'geom'
      : geometryColumns[0];

  const getValue = (key: string) => {
    // the first primary key is not in the properties, so we need to parse it from the feature id
    if (primaryKeys[0] === key) {
      const featureId = feature.id;
      if (typeof featureId === 'string' && featureId.endsWith('n') && !isNaN(Number(featureId.slice(0, -1)))) {
        return BigInt(featureId.slice(0, -1));
      }
      return featureId;
    }

    // the primary geometry key is the geometry property instead of in the properties object
    if (primaryGeometryKey === key) {
      return feature.geometry;
    }

    return feature.properties?.[key];
  };

  for (const entry of schema) {
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

    if (entry.dataType === 'blob') {
      // blobs should be Uint8Array
      const validator = z.instanceof(Uint8Array).nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(
          `Property "${entry.name}" is expected to be a blob (Uint8Array) or null. Got: ${getType(value)}`
        );
      }
      continue;
    }

    if (entry.dataType === 'boolean') {
      const validator = z.boolean().nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(`Property "${entry.name}" is expected to be a boolean or null. Got: ${getType(value)}`);
      }
      continue;
    }

    if (entry.dataType === 'date') {
      const validator = z.instanceof(Temporal.PlainDate).nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(
          `Property "${entry.name}" is expected to be a Temporal.PlainDate or null. Got: ${getType(value)}`
        );
      }
      continue;
    }

    if (entry.dataType === 'float') {
      const validator = z.number().nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(
          `Property "${entry.name}" is expected to be a float (number) or null. Got: ${getType(value)}`
        );
      }
      continue;
    }

    if (entry.dataType === 'geometry') {
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
      continue;
    }

    if (entry.dataType === 'integer') {
      const validator = z.bigint().nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(`Property "${entry.name}" is expected to be a bigint or null. Got: ${getType(value)}`);
      }
      continue;
    }

    if (entry.dataType === 'interval') {
      const validator = z.instanceof(Temporal.Duration).nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(
          `Property "${entry.name}" is expected to be a Temporal.Duration or null. Got: ${getType(value)}`
        );
      }
      continue;
    }

    if (entry.dataType === 'numeric') {
      const validator = z.instanceof(Decimal).nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(`Property "${entry.name}" is expected to be a Decimal or null. Got: ${getType(value)}`);
      }
      continue;
    }

    if (entry.dataType === 'text') {
      const validator = entry.length ? z.string().max(entry.length).nullable() : z.string().nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        if (result.error.issues.some((issue) => issue.code === 'too_big')) {
          errors.push(
            `Property "${entry.name}" exceeds maximum length of ${entry.length}. Got length: ${(value as string).length}`
          );
          continue;
        }

        errors.push(`Property "${entry.name}" is expected to be a string or null. Got: ${getType(value)}`);
      }
      continue;
    }

    if (entry.dataType === 'time') {
      const validator = z.instanceof(Temporal.PlainTime).nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(
          `Property "${entry.name}" is expected to be a Temporal.PlainTime or null. Got: ${getType(value)}`
        );
      }
      continue;
    }

    if (entry.dataType === 'timestamp') {
      const validator = z.instanceof(Temporal.PlainDateTime).nullable();
      const result = z.safeParse(validator, value);
      if (!result.success) {
        errors.push(
          `Property "${entry.name}" is expected to be a Temporal.PlainDateTime or null. Got: ${getType(value)}`
        );
      }
      continue;
    }
  }

  // if there are any properties in the feature that are not in the schema, report them as errors
  for (const key of Object.keys(feature.properties)) {
    if (!checkedProperties.has(key)) {
      errors.push(`Property "${key}" is not defined in the schema.`);
    }
  }

  return { valid: errors.length === 0, errors };
}
