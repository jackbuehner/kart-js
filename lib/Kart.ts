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
  readonly throttledFs: typeof fs;

  readonly data: Data;
  readonly diff: Diff;

  protected constructor(repoDir: string | Path) {
    this.repoDir = repoDir instanceof Path ? repoDir : new Path(repoDir);
    this.throttledFs = Kart.throttledFs;
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

  private static throttledFs = (() => {
    const limit = pLimit(256); // max 256 concurrent file operations

    const wrap = (obj: any) => {
      const wrapper: any = {};
      for (const key in obj) {
        if (typeof obj[key] === 'function') {
          wrapper[key] = (...args: any[]) => {
            // console.log('Accessing fs method:', key);
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

    const http = await (async () => {
      if (process.env.TARGET === 'node') {
        return await import('isomorphic-git/http/node');
      } else {
        return await import('isomorphic-git/http/web');
      }
    })();

    const info = await getRemoteInfo({
      http,
      url,
    });
    const defaultBranch = info.HEAD?.replace('refs/heads/', '') || 'main';

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
        console.log('  Repository is already up to date.');
        return new Kart(dir);
      }

      // hard reset to remote state
      await writeRef({
        fs: this.throttledFs,
        dir,
        ref: `refs/heads/${defaultBranch}`,
        value: remoteHash,
        force: true,
      });

      console.log('Checking out latest changes...');
      await checkout({ fs: this.throttledFs, dir, ref: defaultBranch, force: true, onProgress });
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
