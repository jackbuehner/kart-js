import * as fs from '@zenfs/core';
import { existsSync } from '@zenfs/core';
import { rm } from '@zenfs/core/promises';
import { clone, resolveRef } from 'isomorphic-git';
import { Data } from './commands/data/Data.ts';
import { Diff } from './commands/diff/Diff.ts';
import { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
import { Path } from './utils/index.ts';

interface KartCloneOptions {
  corsProxy?: Parameters<typeof clone>[0]['corsProxy'];
  onProgress?: Parameters<typeof clone>[0]['onProgress'];
}

export class Kart {
  readonly repoDir: Path;

  readonly data: Data;
  readonly diff: Diff;

  protected constructor(repoDir: string | Path) {
    this.repoDir = repoDir instanceof Path ? repoDir : new Path(repoDir);
    this.data = new Data(this);
    this.diff = new Diff(this);
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
      await rm(dir, { recursive: true, force: true });
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
    return resolveRef({ fs, dir: this.repoDir.absolute, ref: 'HEAD' });
  }

  async [Symbol.asyncDispose]() {
    // unregister all listeners
    for await (const [name, value] of this.data) {
      value.dataset.working.off();
    }

    await rm(this.repoDir.absolute, { recursive: true, force: true });
  }

  dispose() {
    return this[Symbol.asyncDispose]();
  }
}
