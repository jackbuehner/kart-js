import { existsSync } from 'fs';
import fs from 'fs/promises';
import git from 'isomorphic-git';
import http from 'isomorphic-git/http/node';
import path from 'path';
import { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';

interface KartCloneOptions {
  corsProxy?: Parameters<typeof git.clone>[0]['corsProxy'];
  onProgress?: Parameters<typeof git.clone>[0]['onProgress'];
}

export class Kart {
  #repoDir: string;

  private constructor(repoDir: string) {
    this.#repoDir = repoDir;
  }

  static async pull(
    url: string,
    dir?: string,
    { corsProxy, onProgress }: KartCloneOptions = {}
  ): Promise<Kart> {
    // infer dir name from repo url if not provided
    if (!dir) {
      const repoNameMatch = url.match(/\/([^\/]+)(\.git)?$/);
      if (!repoNameMatch) {
        throw new Error(`Could not infer directory name from URL: ${url}. Please provide a directory name.`);
      }
      dir = path.join(process.cwd(), repoNameMatch[1]);
    }

    // delete existing directory
    if (existsSync(dir)) {
      await fs.rm(dir, { recursive: true, force: true });
    }

    await git.clone({
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
    return git.resolveRef({ fs, dir: this.#repoDir, ref: 'HEAD' });
  }

  async destroy() {
    // unregister all listeners
    for await (const [name, value] of this) {
      value.dataset.working.off();
    }

    await fs.rm(this.#repoDir, { recursive: true, force: true });
  }

  /**
   * Whether there is a valid table dataset v3 with the given name.
   */
  has(name: string) {
    return TableDatasetV3.isValidDataset(this.#repoDir, name);
  }

  get(name: string) {
    if (!this.has(name)) {
      throw new Error(`Dataset with name "${name}" does not exist or is not a valid table dataset v3.`);
    }

    return new TableDatasetV3(this.#repoDir, name);
  }

  /**
   * An async interator for interating over each validated
   * dataset in the repository.
   */
  private async *entries() {
    const filesOrFolders = await fs.readdir(this.#repoDir);
    const folders = filesOrFolders.filter(async (fileOrFolder) => {
      const stat = await fs.stat(path.join(this.#repoDir, fileOrFolder));
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
