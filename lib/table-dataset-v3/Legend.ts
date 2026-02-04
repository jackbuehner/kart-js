import * as msgpack from '@msgpack/msgpack';
import z from 'zod';
import { FileNotFoundError, InvalidFileContentsError } from '../utils/errors.ts';
import { Enumerable, type Path } from '../utils/index.ts';
import serializer from './serializer.ts';

/**
 * A legend consists of primary key column IDs and non-primary key column IDs.
 * It is used to identify which columns are primary keys and which are not.
 * Additionally, legends are used to uniquely identify columns in a table dataset,
 * even if their names change.
 *
 * A raw legend file is a MesagePack-encoeded array of one of two formats:
 * - When there are no primary keys: `string[]` (an array of non-primary key column IDs)
 * - When there are primary keys:` [string[], string[]]` (a tuple of primary key and non-primary key column IDs)
 *
 * To read a raw legend file, use `Legend.fromFile(filePath)` or `Legend.fromBuffer(buffer)`.
 */
export class Legend {
  /**
   * The identifier for the legend. This is the same as the legend file name.
   */
  readonly id: string;

  /**
   * An array of primary key column IDs. If there are no primary keys, this will be an empty array.
   */
  readonly primaryKeyIds: string[];

  /**
   * An array of non-primary key column IDs. If there are no non-primary keys, this will be an empty array.
   */
  readonly nonPrimaryKeyIds: string[];

  /**
   * Converts the column IDs for a lagend into a Legend instance.
   *
   * @param primaryKeyIds - An array of primary key column IDs. If there are no primary keys, this should be an empty array.
   * @param nonPrimaryKeyIds - An array of non-primary key column IDs. If there are no non-primary keys, this should be an empty array.
   * @param id - The first 20 bytes of the sha256 hash of the legend contents,
   *             represented as a hex string. This is used to uniquely identify the
   *             legend and should be the same as the legend file name.
   */
  constructor(
    primaryKeyIds: string[],
    nonPrimaryKeyIds: string[],
    id?: string,
    { skipValidation = false } = {}
  ) {
    this.primaryKeyIds = primaryKeyIds;
    this.nonPrimaryKeyIds = nonPrimaryKeyIds;

    if (skipValidation && !id) {
      throw new Error('Cannot skip validation without providing an id');
    } else if (skipValidation) {
      this.id = id!;
    } else {
      const buffer = this.toBuffer([primaryKeyIds, nonPrimaryKeyIds]);
      const hash = serializer.hexHash(buffer);
      this.id = hash;

      // if provided, ensure that the id matches the hash of the contents
      if (id && hash !== id) {
        throw new InvalidFileContentsError(
          `Legend has invalid contents: expected SHA-256 hash ${id} but got ${hash}`
        );
      }
    }
  }

  /**
   * An array of all column IDs in the legend.
   *
   * This is the concatenation of `primaryKeyIds` and `nonPrimaryKeyIds`.
   *
   * The `dataIndex` indicates the index of the column in its respective data array.
   *
   * @returns An array of objects containing columnId, isPrimary, and dataIndex.
   */
  get columnIds() {
    return [
      ...this.primaryKeyIds.map((columnId, index) => ({ columnId, isPrimary: true, dataIndex: index })),
      ...this.nonPrimaryKeyIds.map((columnId, index) => ({ columnId, isPrimary: false, dataIndex: index })),
    ];
  }

  /**
   * Encodes the legend into an ArrayBuffer using MessagePack encoding.
   *
   * To convert the ArrayBuffer back into a Legend instance, use `Legend.fromBuffer(buffer)`.
   *
   * @returns An ArrayBuffer containing the MessagePack-encoded legend data.
   */
  toBuffer(): ArrayBuffer;
  toBuffer(data: [string[], string[]]): ArrayBuffer;
  toBuffer(data = [this.primaryKeyIds, this.nonPrimaryKeyIds] as [string[], string[]]) {
    const encoded = msgpack.encode([this.primaryKeyIds, this.nonPrimaryKeyIds]);
    return encoded.buffer.slice(encoded.byteOffset, encoded.byteOffset + encoded.byteLength);
  }

  /**
   * Creates a Legend instance from a MessagePack-encoded ArrayBuffer.
   *
   * @throws {RangeError} If the buffer cannot be decoded or is in an invalid format.
   * @throws {TypeError} If the buffer is not actually an ArrayBuffer or Uint8Array.
   * @throws {msgpack.DecodeError} If the buffer contents cannot be decoded as MessagePack.
   * @throws {InvalidFileContentsError} If the hash of the buffer contents does not match the provided ID (if ID is provided).
   * @throws {z.ZodError} If the decoded contents do not match the expected array structure.
   *
   * @param buffer - The MessagePack-encoded ArrayBuffer containing the legend data.
   * @param id - The expected legend ID (hash). If provided, the function will verify that the hash of the buffer contents matches this ID.
   *             If the hash does not match, an InvalidFileContentsError will be thrown.
   *             If not provided, no hash verification will be performed.
   */
  static fromBuffer(buffer: ArrayBuffer | Uint8Array, id?: string) {
    if (buffer.byteLength === 0) {
      throw new RangeError(`Failed to read legend from buffer: buffer is empty`);
    }

    const hash = serializer.hexHash(buffer);
    if (id && hash !== id) {
      throw new InvalidFileContentsError(
        `Legend buffer has invalid contents: expected SHA-256 hash ${id} but got ${hash}`
      );
    }

    const decoded = msgpack.decode(new Uint8Array(buffer));
    const parsed = legendSchema.parse(decoded);
    return new Legend(parsed.primaryKeyColumns, parsed.nonPrimaryKeyColumns, hash);
  }

  /**
   * Creates a Legend instance from a legend file at the given path.
   *
   * @throws {FileNotFoundError} If the file does not exist at the specified path.
   * @throws {import('../utils/errors.ts').FileReadError} If the file cannot be read.
   * @throws {InvalidFileContentsError} If the hash of the file contents does not match the file name.
   * @throws {RangeError} If the file cannot be read or is in an invalid format.
   * @throws {msgpack.DecodeError} If the file contents cannot be decoded as MessagePack.
   * @throws {z.ZodError} If the decoded contents do not match the expected array structure.
   *
   * @param filePath - The path to the legend file.
   */
  static fromFile(filePath: Path) {
    if (!filePath.exists) {
      throw new FileNotFoundError(`Legend file does not exist at path: ${filePath}`);
    }

    const buffer = filePath.readFileSync();
    if (buffer.length === 0) {
      throw new RangeError(`Failed to read legend file at path: ${filePath}`);
    }

    // the file name should be the same as the hash of the contents
    const id = filePath.name;
    const hash = serializer.hexHash(buffer);
    if (id !== hash) {
      throw new InvalidFileContentsError(
        `Legend file at path ${filePath} has invalid contents: expected SHA-256 hash ${id} but got ${hash}`
      );
    }

    return Legend.fromBuffer(buffer, id);
  }
}

export class Legends extends Enumerable<Legend> {}

const legendSchema = z.tuple([z.string().array(), z.string().array()]).transform((array) => {
  const primaryKeyColumns = array[0];
  const nonPrimaryKeyColumns = array[1];
  return { primaryKeyColumns, nonPrimaryKeyColumns };
});
