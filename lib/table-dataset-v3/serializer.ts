import { sha256 } from '@noble/hashes/sha2.js';
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
      return wkb.toHex();
    }

    if (value instanceof Uint8Array) {
      return value.toHex();
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

/**
 * Returns a proxy that makes an object serializable by adding a `toJSON` method.
 *
 * By default, this is done recursively for nested objects as well.
 *
 * @param data The object to make serializable.
 * @param options.recursive Whether to make nested objects serializable as well. Default is true.
 * @returns A proxy of the original object with a `toJSON` method.
 */
export function makeSerializable<T extends object>(
  data: T,
  { recursive = true } = {}
): T & { toJSON: () => string } {
  return new Proxy(data, {
    get(target, prop, receiver) {
      if (prop === 'toJSON') {
        return () => serializeJson(target);
      }

      const value = Reflect.get(target, prop, receiver);

      if (recursive && value !== null && typeof value === 'object' && value.constructor === Object) {
        return makeSerializable(value);
      }

      return value;
    },
  }) as T & { toJSON: () => string };
}

/**
 * Hashes a string or binary data using SHA-256 and returns the hash as a hex string
 * truncated to the first 160 bits (same as git hashes).
 *
 * This is the JavaScript implementation of the same hashing algorithm used by Kart in Python.
 * @see https://github.com/koordinates/kart/blob/eae35e1d06273d9cd2638cefd5fdc50250971aa4/kart/serialise_util.py#L80-L83
 *
 * @param data The data to hash, provided as a string, Uint8Array, or ArrayBuffer. It will be converted to an ArrayBuffer if it is a string or Uint8Array.
 * @returns The hex string of the hash, truncated to the first 160 bits (20 bytes) (40 characters).
 */
export function hexHash(data: string | Uint8Array | ArrayBuffer) {
  let bytes: Uint8Array<ArrayBuffer>;
  if (data instanceof Uint8Array && data.buffer instanceof ArrayBuffer) {
    bytes = data.slice() as Uint8Array<ArrayBuffer>;
  } else if (data instanceof Uint8Array) {
    bytes = new Uint8Array(data).slice();
  } else if (data instanceof ArrayBuffer) {
    bytes = new Uint8Array(data);
  } else if (typeof data === 'string') {
    bytes = new Uint8Array(Uint8Array.fromHex(data));
  } else {
    throw new TypeError('Data must be a string, Uint8Array, or ArrayBuffer');
  }

  return sha256(bytes).slice(0, 20).toHex();
}

export default {
  stringify,
  parse,
  serializeJson,
  hexHash,
};
