import * as msgpack from '@msgpack/msgpack';
import z from 'zod';
import { FileNotFoundError } from '../utils/errors.ts';
import { Enumerable, type Path } from '../utils/index.ts';

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
   * @param id - The legend ID.
   * @param primaryKeyIds - An array of primary key column IDs. If there are no primary keys, this should be an empty array.
   * @param nonPrimaryKeyIds - An array of non-primary key column IDs. If there are no non-primary keys, this should be an empty array.
   */
  constructor(id: string, primaryKeyIds: string[], nonPrimaryKeyIds: string[]) {
    this.id = id;
    this.primaryKeyIds = primaryKeyIds;
    this.nonPrimaryKeyIds = nonPrimaryKeyIds;
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
   * Creates a Legend instance from a legend file at the given path.
   *
   * @throws {FileNotFoundError} If the file does not exist at the specified path.
   * @throws {import('../utils/errors.ts').FileReadError} If the file cannot be read.
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

    const decoded = msgpack.decode(buffer);
    const parsed = legendSchema.parse(decoded);
    const id = filePath.name;
    return new Legend(id, parsed.primaryKeyColumns, parsed.nonPrimaryKeyColumns);
  }
}

export class Legends extends Enumerable<Legend> {}

const legendSchema = z.tuple([z.string().array(), z.string().array()]).transform((array) => {
  const primaryKeyColumns = array[0];
  const nonPrimaryKeyColumns = array[1];
  return { primaryKeyColumns, nonPrimaryKeyColumns };
});
