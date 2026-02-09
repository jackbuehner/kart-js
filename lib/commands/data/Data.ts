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
  async has(name: string) {
    return this.datatsets.has(name) || (await TableDatasetV3.isValidDataset(this.core.repoTree, name));
  }

  /**
   * Gets the dataset with the given name if it exists and is a valid table dataset v3.
   */
  async get(name: string) {
    if (this.datatsets.has(name)) {
      return this.datatsets.get(name)!;
    }

    if (!(await this.has(name))) {
      return null;
    }

    const newDataset = await TableDatasetV3.create(this.core, name);
    this.datatsets.set(name, newDataset);
    return newDataset;
  }

  /**
   * Deletes the dataset with the given name from the repository.
   */
  async delete(name: string) {
    if (!(await this.has(name))) {
      return false;
    }

    const dataset = (await this.get(name))!;
    await dataset.tree.rm();
    return this.datatsets.delete(name);
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
      if (await this.has(folder)) {
        yield [
          folder,
          {
            type: 'table-dataset-v3',
            dataset: await this.get(folder),
          },
        ] as DatasetEntry;
      }
    }
  }

  private *loadedEntries() {
    const path = new Path(this.core.repoDir.absolute);
    const folders = path.readDirectorySync().filter((entry) => entry.isDirectory);

    for (const folder of folders) {
      if (this.datatsets.has(folder.name)) {
        yield [
          folder.name,
          {
            type: 'table-dataset-v3',
            dataset: this.datatsets.get(folder.name)!,
          },
        ] as DatasetEntry;
      }
    }
  }

  [Symbol.asyncIterator]() {
    return this.entries();
  }

  /**
   * Removes all event listeners from the loaded datasets.
   */
  removeAllEventListeners() {
    for (const [name, value] of this.loadedEntries()) {
      value.dataset.working.off();
    }
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
