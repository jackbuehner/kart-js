import * as JSON from '@ungap/raw-json';
import * as devalue from 'devalue';
import { Temporal } from 'temporal-polyfill';
import { convertGeometryToWkb } from '../utils/features/index.ts';

export function stringify(value: unknown) {
  return devalue.stringify(value, {
    'Temporal.PlainYearMonth': (value: unknown) => value instanceof Temporal.PlainYearMonth && value.toString(),
    'Temporal.PlainMonthDay': (value: unknown) => value instanceof Temporal.PlainMonthDay && value.toString(),
    'Temporal.PlainDate': (value: unknown) => value instanceof Temporal.PlainDate && value.toString(),
    'Temporal.PlainTime': (value: unknown) => value instanceof Temporal.PlainTime && value.toString(),
    'Temporal.PlainDateTime': (value: unknown) => value instanceof Temporal.PlainDateTime && value.toString(),
    'Temporal.ZonedDateTime': (value: unknown) => value instanceof Temporal.ZonedDateTime && value.toString(),
    'Temporal.Instant': (value: unknown) => value instanceof Temporal.Instant && value.toString(),
    'Temporal.Duration': (value: unknown) => value instanceof Temporal.Duration && value.toString(),
  });
}

export function parse(str: string) {
  return devalue.parse(str, {
    'Temporal.PlainYearMonth': (str: string) => Temporal.PlainYearMonth.from(str),
    'Temporal.PlainMonthDay': (str: string) => Temporal.PlainMonthDay.from(str),
    'Temporal.PlainDate': (str: string) => Temporal.PlainDate.from(str),
    'Temporal.PlainTime': (str: string) => Temporal.PlainTime.from(str),
    'Temporal.PlainDateTime': (str: string) => Temporal.PlainDateTime.from(str),
    'Temporal.ZonedDateTime': (str: string) => Temporal.ZonedDateTime.from(str),
    'Temporal.Instant': (str: string) => Temporal.Instant.from(str),
    'Temporal.Duration': (str: string) => Temporal.Duration.from(str),
  });
}

/**
 * Serializes feature values according to Kart's serialization rules.
 *
 * @see https://github.com/koordinates/kart/blob/eae35e1d06273d9cd2638cefd5fdc50250971aa4/kart/tabular/feature_output.py#L33-L55
 * @see https://github.com/koordinates/kart/blob/eae35e1d06273d9cd2638cefd5fdc50250971aa4/kart/output_util.py#L57-L79
 *
 * @remarks
 * - geometry objects are serialized to WKB blobs (as base64 strings) with little-endian byte order, SRS ID 0, and envelopes for non-point geometries.
 * - `Uint8Array` blobs are serialized to base64 strings.
 * - `Decimal` (numeric) (double) values are serialized to strings to preserve precision.
 * - `Temporal` date, time, datatime, and duration objects are serialized to ISO 8601 strings.
 * - Other values are serialized using standard JSON serialization.
 *
 * @param value
 * @returns
 */
export function serializeJson(value: unknown): string {
  function replacer(_key: string, value: unknown): unknown {
    if (typeof value === 'object' && value !== null && 'type' in value && 'coordinates' in value) {
      const wkb = convertGeometryToWkb(value as GeoJSON.Geometry);
      return Buffer.from(wkb).toString('hex');
    }

    if (value instanceof Uint8Array) {
      return Buffer.from(value).toString('hex');
    }

    if (typeof value === 'bigint') {
      // By using JSON.rawJSON, we can insert the bigint as a raw value in the JSON output.
      // This allows us to store the bigint as a number in JSON without quotes.
      // Note: JavaScript parsers may not handle reading the number back correctly if it exceeds Number.MAX_SAFE_INTEGER.
      return JSON.rawJSON(value.toString());
    }

    return value;
  }

  return JSON.stringify(value, replacer);
}

export function makeSerializeable<T extends object>(data: T) {
  return new Proxy(data, {
    get(target, prop, receiver) {
      if (prop === 'toJSON') {
        return () => serializeJson(target);
      }
      return Reflect.get(target, prop, receiver);
    },
  }) as T & { toJSON: () => string };
}

export default {
  stringify,
  parse,
  serializeJson,
};
