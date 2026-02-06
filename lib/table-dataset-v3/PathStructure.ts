import * as msgpack from '@msgpack/msgpack';
import { sha256 } from '@noble/hashes/sha2.js';
import z from 'zod';
import { FileNotFoundError, InvalidFileContentsError } from '../utils/errors.ts';
import type { Path } from '../utils/index.ts';

/**
 * The path structure defines the folder structure and file
 * naming convention for feature files in a Kart Table Dataset V3.
 *
 * To get the encoded path structure for a specific feature, use the
 * `getEid` method with the feature's primary key values.
 *
 * To read a path structure file, use `PathStructure.fromFile(filePath)`.
 */
export class PathStructure {
  readonly scheme: PathStructureData['scheme'];
  readonly branches: PathStructureData['branches'];
  readonly levels: PathStructureData['levels'];
  readonly encoding: PathStructureData['encoding'];

  /**
   * Creates a new PathStructure instance with the specified scheme, branches, levels, and encoding.
   *
   * Values will be validated against the path structure schema to ensure they are valid.
   * @see pathStructureSchema
   */
  constructor(
    scheme: typeof PathStructure.prototype.scheme,
    branches: typeof PathStructure.prototype.branches,
    levels: number,
    encoding: typeof PathStructure.prototype.encoding
  ) {
    this.scheme = scheme;
    this.branches = branches;
    this.levels = levels;
    this.encoding = encoding;

    // validate the instance using the schema
    pathStructureSchema.parse({
      scheme: this.scheme,
      branches: this.branches,
      levels: this.levels,
      encoding: this.encoding,
    });
  }

  /**
   * Gets the encoded path structure (encoded ID) for a given set of primary key values
   * based on the path structure's scheme, branches, levels, and encoding.
   *
   * The encoded path structure is the folder structure and file name for a feature
   * file in a Kart Table Dataset V3.
   *
   * For more details and examples, refer to the feature paths documentation.
   * @see https://docs.kartproject.org/en/latest/pages/development/table_v3.html#feature-paths
   */
  getEid(primaryKeyValues: unknown[]) {
    if (this.scheme === 'int') {
      const isCorrectLength = primaryKeyValues.length === 1;
      if (!isCorrectLength) {
        throw new Error(
          `Path structure with 'int' scheme requires exactly one primary key value. Got ${primaryKeyValues.length}.`
        );
      }

      const isInteger = typeof primaryKeyValues[0] === 'bigint' || Number.isInteger(primaryKeyValues[0]);
      if (!isInteger) {
        throw new Error(
          `Path structure with 'int' scheme requires primary key value to be an integer or bigint. Got: ${primaryKeyValues[0]}`
        );
      }
    }

    if (primaryKeyValues.length === 0) {
      throw new Error('At least one primary key value is required to compute the path structure.');
    }

    const fileName = msgpack.encode(primaryKeyValues).toBase64();

    if (this.scheme === 'int') {
      const integer = BigInt(primaryKeyValues[0] as number | bigint);
      const folderStructure = this.encodeIntegerAsFolderStructure(integer);
      return `${folderStructure}${fileName}`;
    }

    const folderStructure = this.encodeArrayAsFolderStructure(primaryKeyValues);
    return `${folderStructure}${fileName}`;
  }

  /**
   * Encodes an array of values as a folder structure based on the path structure's levels and encoding.
   *
   * The array values are encoded with MessagePack and then hashed with SHA-256.
   *
   * @param values - The array of values to encode.
   *
   * @returns The folder structure as a string with slashes separating each level.
   */
  private encodeArrayAsFolderStructure(values: unknown[]) {
    const packed = msgpack.encode(values);
    const hashed = sha256(packed);

    // We only need one byte per level in the path structure.
    // (two hex characters or one base64 character)
    const clipped = hashed.slice(0, this.levels);

    let encoded: string;
    if (this.encoding === 'base64') {
      encoded = new Uint8Array(clipped).toBase64().replaceAll('=', '').padStart(this.levels, 'A');
    } else {
      encoded = new Uint8Array(clipped).toHex().padStart(this.levels * 2, '0');
    }

    return this.charSlash(encoded.slice(0, this.levels * (this.encoding === 'base64' ? 1 : 2)));
  }

  /**
   * Encodes a bigint as a folder structure
   * based on the path structure's levels and encoding.
   *
   * @throws {Error} If the encoding fails.
   *
   * @param integer - The bigint to encode.
   *
   * @returns The folder structure as a string with slashes separating each level.
   */
  private encodeIntegerAsFolderStructure(integer: bigint) {
    // We do not use the last encoded character in the path structure.
    // The last character changes with every increment of the integer,
    // which would cause a new folder for every integer.
    const maxLength = this.levels + 1;

    let encoded: string;
    if (this.encoding === 'base64') {
      encoded = this.bigIntTobase64(integer, maxLength).padStart(maxLength, 'A');
    } else {
      encoded = this.bigIntToHex(integer, maxLength).padStart(maxLength, '0');
    }

    return this.charSlash(encoded.slice(0, this.levels * (this.encoding === 'base64' ? 1 : 2)));
  }

  /**
   * Returns a new string with a forward slash character inserted after each character in the input string.
   * For example, "abc" becomes "a/b/c/".
   */
  private charSlash(string: string) {
    return string
      .split('')
      .map((char) => `${char}/`)
      .join('');
  }

  /**
   * Converts a bigint to a base64-encoded string
   * that can be used to form path segments.
   *
   * The resulting string is in big-endian format.
   *
   * @throws {Error} If the resulting base64 string exceeds the specified maximum length.
   *
   * @param integer - The bigint to encode.
   * @param maxLength - The maximum allowed length of the resulting base64 string.
   */
  private bigIntTobase64(integer: bigint, maxLength: number) {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    let result = '';

    if (integer === 0n) {
      return chars[0];
    }

    // process each 6-bit chunk of the integer
    // base64 = 2^6 = 64
    while (integer > 0n) {
      const remainder = integer % 64n;
      result = chars[Number(remainder)] + result; // prepend for big-endian
      integer = integer / 64n;
    }

    if (result.length > maxLength) {
      throw new Error(`Encoded base64 string length ${result.length} exceeds maximum length of ${maxLength}.`);
    }

    return result;
  }

  /**
   * Converts a bigint to a hex-encoded string
   * that can be used to form path segments.
   *
   * The resulting string is in big-endian format.
   *
   * @throws {Error} If the resulting hex string exceeds the specified maximum length.
   *
   * @param integer - The bigint to encode.
   * @param maxLength - The maximum allowed length of the resulting hex string.
   */
  private bigIntToHex(integer: bigint, maxLength: number) {
    const chars = '0123456789abcdef';
    let result = '';

    if (integer === 0n) {
      return chars[0];
    }

    // process each 4-bit chunk of the integer
    // hex = 2^4 = 16
    while (integer > 0n) {
      const remainder = integer % 16n;
      result = chars[Number(remainder)] + result; // prepend for big-endian
      integer = integer / 16n;
    }

    if (result.length > maxLength) {
      throw new Error(`Encoded hex string length ${result.length} exceeds maximum length of ${maxLength}.`);
    }

    return result;
  }

  toObject() {
    return {
      scheme: this.scheme,
      branches: this.branches,
      levels: this.levels,
      encoding: this.encoding,
    };
  }

  /**
   * Creates a PathStructure instance from a `path-structure.json` file at the given path.
   *
   * @throws {FileNotFoundError} If the file does not exist at the specified path.
   * @throws {import('../utils/errors.ts').FileReadError} If the file cannot be read.
   * @throws {InvalidFileContentsError} If the file contents cannot be parsed as valid JSON.
   * @throws {z.ZodError} If the decoded contents do not match the expected array structure.
   *
   * @param filePath - The path to the path structure file.
   */
  static fromFile(path: Path) {
    if (!path.exists || !path.isFile) {
      throw new FileNotFoundError(`Path structure file does not exist at path: ${path}`);
    }

    const fileContents = path.readFileSync({ encoding: 'utf-8' });
    let fileJson: unknown;
    try {
      fileJson = JSON.parse(fileContents);
    } catch (error) {
      const exposedError = new InvalidFileContentsError(
        `Failed to parse path structure file at path: ${path}. Invalid JSON format.`
      );
      exposedError.cause = error;
      throw exposedError;
    }
    const pathStructure = pathStructureSchema.parse(fileJson);
    return new PathStructure(
      pathStructure.scheme,
      pathStructure.branches,
      pathStructure.levels,
      pathStructure.encoding
    );
  }
}

type PathStructureData = z.infer<typeof pathStructureSchema>;
export type PathStructureEid = string;

const pathStructureSchema = z
  .object({
    scheme: z.literal(['int', 'msgpack/hash']),
    branches: z.literal([16, 64, 256]),
    levels: z.int().positive().gt(0),
    encoding: z.literal(['base64', 'hex']),
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
