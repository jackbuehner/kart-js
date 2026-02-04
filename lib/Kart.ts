import { existsSync } from '@zenfs/core';
import * as fs from '@zenfs/core/promises';
import { clone, resolveRef } from 'isomorphic-git';
import type { KartDiff } from './table-dataset-v3/diffs.js';
import { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
import { Path } from './utils/index.ts';

interface KartCloneOptions {
  corsProxy?: Parameters<typeof clone>[0]['corsProxy'];
  onProgress?: Parameters<typeof clone>[0]['onProgress'];
}

export class Kart {
  #repoDir: Path;

  protected constructor(repoDir: string | Path) {
    this.#repoDir = repoDir instanceof Path ? repoDir : new Path(repoDir);
  }

  protected static inferRepoNameFromUrl(url: string): string {
    const repoNameMatch = url.match(/\/([^\/]+)(\.git)?$/);
    if (!repoNameMatch) {
      throw new Error(`Could not infer repository name from URL: ${url}. Please provide a directory name.`);
    }
    return repoNameMatch[1];
  }

  static async pull(
    url: string,
    dir?: string,
    { corsProxy, onProgress }: KartCloneOptions = {}
  ): Promise<Kart> {
    // infer dir name from repo url if not provided
    if (!dir) {
      dir = Kart.inferRepoNameFromUrl(url);
    }

    // delete existing directory
    if (existsSync(dir)) {
      await fs.rm(dir, { recursive: true, force: true });
    }

    const http = await (async () => {
      if (process.env.TARGET === 'node') {
        return await import('isomorphic-git/http/node');
      } else {
        return await import('isomorphic-git/http/web');
      }
    })();

    await clone({
      fs,
      http,
      dir,
      url,
      corsProxy,
      depth: 1, // only get latest commit
      onProgress,
    });

    return new Kart(dir);
  }

  async getCurrentCommit() {
    return resolveRef({ fs, dir: this.#repoDir.absolute, ref: 'HEAD' });
  }

  async [Symbol.asyncDispose]() {
    // unregister all listeners
    for await (const [name, value] of this) {
      value.dataset.working.off();
    }

    await fs.rm(this.#repoDir.absolute, { recursive: true, force: true });
  }

  dispose() {
    return this[Symbol.asyncDispose]();
  }

  /**
   * Whether there is a valid table dataset v3 with the given name.
   */
  has(name: string) {
    return TableDatasetV3.isValidDataset(this.#repoDir, name);
  }

  private datatsets = new Map<string, TableDatasetV3>();

  get(name: string) {
    if (!this.has(name)) {
      throw new Error(`Dataset with name "${name}" does not exist or is not a valid table dataset v3.`);
    }

    if (this.datatsets.has(name)) {
      return this.datatsets.get(name)!;
    }

    const newDataset = new TableDatasetV3(this.#repoDir, name);
    this.datatsets.set(name, newDataset);
    return newDataset;
  }

  /**
   * An async interator for interating over each validated
   * dataset in the repository.
   */
  private async *entries() {
    const filesOrFolders = await fs.readdir(this.#repoDir.absolute);
    const folders = filesOrFolders.filter(async (fileOrFolder) => {
      const stat = await fs.stat(Path.join(this.#repoDir.absolute, fileOrFolder).absolute);
      return stat.isDirectory();
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

  /**
   * Generates a diff object representing the changes made to all datasets in the Kart repository.
   *
   * @returns A promise that resolves to a `KartDiff.HexWkB.v1.Diff` object containing the diffs for all datasets.
   */
  async toDiff() {
    const diffObject: KartDiff.HexWkB.v1.Diff = {};

    for await (const [name, value] of this) {
      const datasetDiff = value.dataset.working.diff?.['kart.diff/v1+hexwkb'];
      if (datasetDiff) {
        diffObject[name] = datasetDiff[name];
      }
    }

    return {
      'kart.patch/v1': {
        base: await this.getCurrentCommit(),
        crs: 'EPSG:4326',
      },
      'kart.diff/v1+hexwkb': diffObject,
    };
  }
}

interface TableDatasetV3Value {
  type: 'table-dataset-v3';
  dataset: TableDatasetV3;
}

type DatasetEntry = [string, TableDatasetV3Value];
