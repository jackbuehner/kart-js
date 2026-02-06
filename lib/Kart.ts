import * as fs from '@zenfs/core';
import { existsSync } from '@zenfs/core';
import { rm } from '@zenfs/core/promises';
import {
  checkout,
  clone,
  getRemoteInfo,
  fetch as gitFetch,
  listRemotes,
  resolveRef,
  STAGE,
  TREE,
  walk,
  WORKDIR,
  writeRef,
} from 'isomorphic-git';
import pLimit from 'p-limit';
import { Data } from './commands/data/Data.ts';
import { Diff } from './commands/diff/Diff.ts';
import { Path } from './utils/index.ts';

interface KartCloneOptions {
  corsProxy?: Parameters<typeof clone>[0]['corsProxy'];
  onProgress?: Parameters<typeof clone>[0]['onProgress'];
}

export class Kart {
  readonly repoDir: Path;
  readonly internalDir: Path;
  readonly throttledFs: typeof fs;

  readonly data: Data;
  readonly diff: Diff;

  protected constructor(repoDir: string | Path) {
    this.repoDir = repoDir instanceof Path ? repoDir : new Path(repoDir);
    this.throttledFs = Kart.throttledFs;

    this.internalDir = this.repoDir.parentPath!.join('.kartjs');
    this.internalDir.makeDirectory();

    this.data = new Data(this);
    this.diff = new Diff(this);
  }

  protected static inferRepoNameFromUrl(url: string): string {
    const repoNameMatch = url.match(/\/([^\/]+)(\.git)?$/);
    if (!repoNameMatch) {
      throw new Error(`Could not infer repository name from URL: ${url}. Please provide a directory name.`);
    }
    return repoNameMatch[1]!;
  }

  private static throttledFs = (() => {
    const limit = pLimit(256); // max 256 concurrent file operations

    const wrap = (obj: any) => {
      const wrapper: any = {};
      for (const key of Object.keys(obj)) {
        if (typeof obj[key] === 'function') {
          wrapper[key] = (...args: any[]) => {
            return limit(() => obj[key](...args));
          };
        }
      }
      return wrapper;
    };
    return {
      ...wrap(fs),
      promises: wrap(fs.promises),
    };
  })();

  static async pull(
    url: string,
    dir?: string,
    { corsProxy, onProgress }: KartCloneOptions = {}
  ): Promise<Kart> {
    // infer dir name from repo url if not provided
    if (!dir) {
      dir = Kart.inferRepoNameFromUrl(url);
    }

    // always use a subfolder called "repository"
    dir = new Path(dir).join('repository').absolute;

    const http = await (async () => {
      if (process.env.TARGET === 'node') {
        return await import('isomorphic-git/http/node');
      } else {
        return await import('isomorphic-git/http/web');
      }
    })();

    console.log(`Pulling repository from ${url} into ${dir}...`);
    const info = await getRemoteInfo({
      http,
      corsProxy,
      url,
    });
    const defaultBranch = info.HEAD?.replace('refs/heads/', '') || 'main';
    console.debug(`  Default branch is "${defaultBranch}"`);

    let repoExists = false;
    if (existsSync(dir)) {
      // check if the existing directory is a git repository with the same remote url
      const remoteInfo = await listRemotes({ fs, dir });
      const origin = remoteInfo.find((remote) => remote.remote === 'origin');
      const isSameRepo = origin?.url === url;

      if (!isSameRepo) {
        // delete existing directory
        await rm(dir, { recursive: true, force: true });
      } else {
        repoExists = true;
      }
    }

    // if repo exists, switch to the default branch and replace it with the latest changes
    if (repoExists) {
      console.log('Fetching latest changes for existing repository...');
      await gitFetch({
        fs: this.throttledFs,
        http,
        dir,
        corsProxy,
        singleBranch: true,
        depth: 1,
        ref: defaultBranch,
        onProgress,
      });

      const remoteHash = await resolveRef({
        fs: this.throttledFs,
        dir,
        ref: `refs/remotes/origin/${defaultBranch}`,
      });
      const localHash = await resolveRef({
        fs: this.throttledFs,
        dir,
        ref: defaultBranch,
      });
      if (remoteHash === localHash) {
        console.log('  Checking for local changes...');
        const foundDirtyFilePath = await this.getFirstFoundDirtyFile(dir);
        if (foundDirtyFilePath) {
          console.log(`    Found mismatch for file: ${foundDirtyFilePath}`);
          console.log('  Discarding local changes...');
        } else {
          console.log('  No local changes found. Repository is up to date.');
          return new Kart(dir);
        }
      }

      // hard reset to remote state
      await writeRef({
        fs: this.throttledFs,
        dir,
        ref: `refs/heads/${defaultBranch}`,
        value: remoteHash,
        force: true,
      });

      await checkout({ fs: this.throttledFs, dir, ref: defaultBranch, force: true, onProgress });
      await new Path(dir).join('.kartjs').rm({ recursive: true, force: true }); // clear internal directory to remove any stale data from previous version
      return new Kart(dir);
    }

    // otherwise, clone the repository
    console.log('Cloning repository...');
    await clone({
      fs: this.throttledFs,
      http,
      dir,
      url,
      corsProxy,
      depth: 1, // only get latest commit
      singleBranch: true,
      onProgress,
    });
    return new Kart(dir);
  }

  /**
   * Searches the given directory for the first file that has uncommitted changes.
   *
   * This is useful for checking whether a directory is dirty without needing
   * to scan entire directory. If the directory is clean, this method will
   * still scan the entire directory and its subdirectories.
   */
  private static async getFirstFoundDirtyFile(dir: string) {
    let dirtyFilePath: string | undefined = undefined;
    try {
      await walk({
        fs: this.throttledFs,
        dir,
        trees: [TREE({ ref: 'HEAD' }), WORKDIR(), STAGE()],
        map: async (filepath, [head, workdir, stage]) => {
          if (filepath.startsWith('.git/')) {
            return;
          }

          // Skip directories - status() is intended for files
          // In walk, if an entry is a directory, its type is 'tree'
          const [headType, workdirType, stageType] = await Promise.all([
            head?.type(),
            workdir?.type(),
            stage?.type(),
          ]);
          if (headType === 'tree' || workdirType === 'tree' || stageType === 'tree') {
            return;
          }

          // Compare Object IDs to detect changes
          const [headOid, workdirOid, stageOid] = await Promise.all([
            head?.oid(),
            workdir?.oid(),
            stage?.oid(),
          ]);

          let reason = 'unmodified';
          if (!headOid && (workdirOid || stageOid)) {
            reason = 'added';
          } else if (headOid && !workdirOid && !stageOid) {
            reason = 'deleted';
          } else if (headOid && workdirOid !== headOid) {
            reason = 'modified';
          } else if (headOid && stageOid !== headOid) {
            reason = 'staged';
          }

          if (reason !== 'unmodified') {
            dirtyFilePath = filepath;
            // console.log(`  Found dirty file: ${filepath}`);
            // console.log(`    Reason: ${reason}`);

            throw new Error('__Interrupt__');
          }

          return filepath;
        },
      });
    } catch (error) {
      if (!(error instanceof Error) || error.message !== '__Interrupt__') {
        throw error;
      }
    }
    return dirtyFilePath as string | undefined;
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
