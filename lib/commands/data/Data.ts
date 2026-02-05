import { readdir, stat } from '@zenfs/core/promises';
import type { Kart } from '../../Kart.ts';
import { TableDatasetV3 } from '../../table-dataset-v3/TableDatasetV3.ts';
import { Path } from '../../utils/index.ts';

export class Data {
  private core: Kart;
  private datatsets = new Map<string, TableDatasetV3>();

  constructor(core: Kart) {
    this.core = core;
  }

  /**
   * Whether there is a valid table dataset v3 with the given name.
   */
  has(name: string) {
    return TableDatasetV3.isValidDataset(this.core.repoDir, name);
  }

  /**
   * Gets the dataset with the given name if it exists and is a valid table dataset v3.
   */
  get(name: string) {
    if (!this.has(name)) {
      throw new Error(`Dataset with name "${name}" does not exist or is not a valid table dataset v3.`);
    }

    if (this.datatsets.has(name)) {
      return this.datatsets.get(name)!;
    }

    const newDataset = new TableDatasetV3(this.core.repoDir, name);
    this.datatsets.set(name, newDataset);
    return newDataset;
  }

  /**
   * An async interator for interating over each validated
   * dataset in the repository.
   */
  private async *entries() {
    const filesOrFolders = await readdir(this.core.repoDir.absolute);
    const folders = filesOrFolders.filter(async (fileOrFolder) => {
      const stats = await stat(Path.join(this.core.repoDir.absolute, fileOrFolder).absolute);
      return stats.isDirectory();
    });

    for (const folder of folders) {
      if (this.has(folder)) {
        yield [
          folder,
          {
            type: 'table-dataset-v3',
            dataset: this.get(folder),
          },
        ] as DatasetEntry;
      }
    }
  }

  [Symbol.asyncIterator]() {
    return this.entries();
  }

  async toObject() {
    return Object.fromEntries(await Array.fromAsync(this));
  }

  async toArray() {
    return Array.fromAsync(this).then((entries) => entries.map(([, value]) => value.dataset));
  }
}

type DatasetEntry = [string, TableDatasetV3Value];

interface TableDatasetV3Value {
  type: 'table-dataset-v3';
  dataset: TableDatasetV3;
}
