import * as fs from '@zenfs/core';
import { rm } from '@zenfs/core/promises';
import {
  addRemote,
  clone,
  getConfigAll,
  getRemoteInfo,
  fetch as gitFetch,
  init,
  listRemotes,
  resolveRef,
  setConfig,
  TREE,
  walk,
} from 'isomorphic-git';
import pLimit from 'p-limit';
import { applyUpdate, encodeStateAsUpdate, Doc as YDoc } from 'yjs';
import { Data } from './commands/data/Data.ts';
import { Diff } from './commands/diff/Diff.ts';
import { debounce, Path } from './utils/index.ts';
import { GitTree } from './utils/Path.ts';

interface KartCloneOptions {
  ref?: string;
  corsProxy?: Parameters<typeof clone>[0]['corsProxy'];
  onProgress?: Parameters<typeof clone>[0]['onProgress'];
}

export class Kart {
  readonly repoDir: Path;
  readonly repoTree: GitTree;
  readonly throttledFs: typeof fs;

  readonly roomName: string;
  readonly ydoc: YDoc;

  readonly data: Data;
  readonly diff: Diff;

  protected constructor(dir: Path, refObjectId: string, roomName: string) {
    // TODO: add a locking mechnanism to prevent multiple Kart instances from using the same repoDir simultaneously
    // TODO: add a locking mechanism to prevent multiple Kart instances from using the same room simultaneously
    this.repoDir = dir;
    if (!this.repoDir) {
      throw new Error('Invalid repository directory');
    }

    const gitdir = this.repoDir.join('.kartjs');
    if (!gitdir.exists) {
      throw new Error(`Git directory not found at ${gitdir.absolute}`);
    }
    this.roomName = roomName;
    this.repoTree = new GitTree(gitdir, gitdir, roomName, refObjectId);

    this.ydoc = Kart.loadYDoc(gitdir, roomName, refObjectId);

    this.throttledFs = Kart.throttledFs;
    this.data = new Data(this);
    this.diff = new Diff(this);
  }

  /**
   * Loads or creates a Y.Doc for the given room in the specified git directory.
   */
  private static loadYDoc(gitdir: Path, roomName: string, refObjectId: string) {
    const ydoc = new YDoc();

    const roomPath = gitdir.join('rooms', roomName);
    const ydocPath = roomPath.join(`${roomName}.ydoc`);
    const refObjectIdPath = roomPath.join('ref');
    roomPath.makeDirectory({ recursive: true });

    // if a ydoc already exists, the record of it's starting ref must also exist
    // and match the refObjectId parameter to ensure that the correct ydoc is loaded
    // for the current state of the repository.
    if (ydocPath.exists) {
      if (!refObjectIdPath.exists) {
        throw new Error(
          `Y.Doc found at ${ydocPath.absolute} but missing ref record at ${refObjectIdPath.absolute}.`
        );
      }

      const storedRefObjectId = refObjectIdPath.readFileSync({ encoding: 'utf-8' });
      if (storedRefObjectId.trim() !== refObjectId.trim()) {
        throw new Error(
          `Y.Doc found at ${ydocPath.absolute} but ref record at ${refObjectIdPath.absolute} does not match the expected refObjectId.\nExpected: ${refObjectId}\nFound: ${storedRefObjectId}`
        );
      }
    }

    // restore persisted ydoc state if it exists
    if (ydocPath.exists) {
      try {
        const storedYDocData = ydocPath.readFileSync();
        applyUpdate(ydoc, new Uint8Array(storedYDocData));
      } catch (error) {
        const err = new Error(
          `Failed to load persisted Y.Doc state from ${ydocPath.absolute}.\nDid you previously stop the process while it was writing the file? Trying inspecting the file with https://inspector.yjs.dev.`,
          { cause: error }
        );
        err.name = 'YDocLoadError';
        throw err;
      }
    }

    // When we create a new YDoc, we need to record its associated refObjectId.
    // All changes stored in the YDoc are based on the state of the repository at that ref.
    else {
      refObjectIdPath.writeFileSync(refObjectId + '\n', { encoding: 'utf-8' });
    }

    // store the ydoc state on disk whenever it is updated
    const save = async () => {
      const encodedState = encodeStateAsUpdate(ydoc);
      await ydocPath.writeFile(encodedState);
    };
    const debouncedSave = debounce(save, 1000);
    ydoc.on('update', (update: Uint8Array, origin: any) => {
      debouncedSave();
    });

    return ydoc;
  }

  /**
   * Pulls a repository from the given URL into a local directory.
   *
   * To attach to an existing local repository, use `Kart.attach(url, dir)` instead.
   *
   * @remarks
   * If the directory already exists, it must match the given URL.
   * Otherwise, the existing directory will be deleted and re-cloned from the URL.
   *
   * If the directory does not exist, a new bare repository will be cloned from the URL.
   *
   * In either case, the repository will be configured to populate both `refs/remotes/origin/*` and `refs/heads/*`
   * on fetch, ensuring that local branches are always updated to match the remote branches.
   *
   * @param url - The remote repository URL to pull from.
   * @param roomName - The name of the kartjs room to use.
   * @param dir - The local directory to pull into. If not provided, it will be inferred from the URL.
   * @param options - Additional options for cloning and fetching.
   * @returns A new instance of `Kart` attached to the pulled repository.
   */
  static async pull(
    url: string | URL,
    roomName: string,
    dir?: string,
    { corsProxy, onProgress, ref = 'refs/remotes/origin/HEAD' }: KartCloneOptions = {}
  ) {
    if (url instanceof URL) {
      url = url.href;
    }

    // infer dir name from repo url if not provided
    if (!dir) {
      dir = Kart.inferRepoNameFromUrl(url);
    }

    // resolve the ref to its sha

    // always use a subfolder called ".kartjs" for the git repository contents
    const gitdir = new Path(dir).join('.kartjs');

    await this.initAndFetchBareRepo(url, gitdir, { corsProxy, onProgress });
    const refObjectId = await resolveRef({ fs, gitdir: gitdir.absolute, ref });
    await this.resetIndex(gitdir, roomName, refObjectId);
    return new Kart(new Path(dir), refObjectId, roomName);
  }

  /**
   * Attaches to an existing local repository at the given directory.
   * The repository must already exist and be a valid kartjs repository.
   *
   * @remarks
   * The repository at the given directory must have a remote named "origin" with a URL that matches the provided URL.
   * If the repository does not exist, is not a valid kartjs repository, or does not have a matching remote, an error will be thrown.
   *
   * To pull from a remote repository and clone it into a local directory, use `Kart.pull(url, dir)` instead.
   *
   * @param url - The expected remote repository URL. The repository at the given directory must have a remote named "origin" with this URL.
   * @param dir - The local directory to attach to. This directory must already exist and contain a valid kartjs repository.
   * @param roomName - The name of the kartjs room to use. If the room already exists, this instance will share state with it (not recommended).
   * @param ref - The git ref to read the initial state from. Defaults to 'refs/remotes/origin/HEAD'.
   * @returns A new instance of `Kart` attached to the existing repository.
   */
  static async attach(
    url: string | URL,
    dir: string | Path,
    roomName: string,
    ref = 'refs/remotes/origin/HEAD'
  ) {
    if (!(dir instanceof Path)) {
      dir = new Path(dir);
    }
    if (!dir.exists) {
      throw new Error(`Directory "${dir}" does not exist.`);
    }

    // expect .kartjs to contain a matching git repository
    const gitdir = dir.join('.kartjs');
    if (!gitdir.exists) {
      throw new Error(`No git repository found at "${gitdir.absolute}".`);
    }

    const remoteInfo = await listRemotes({ fs, gitdir: gitdir.absolute });
    const origin = remoteInfo.find((remote) => remote.remote === 'origin');
    if (!origin) {
      throw new Error(`No remote named "origin" found in repository at "${gitdir}".`);
    }
    if (origin.url !== url.toString()) {
      throw new Error(`Remote "origin" URL "${origin.url}" does not match expected URL "${url}".`);
    }

    const refObjectId = await resolveRef({ fs, gitdir: gitdir.absolute, ref });
    return new Kart(dir, refObjectId, roomName);
  }

  private static http = (async () => {
    if (process.env.TARGET === 'node') {
      return await import('isomorphic-git/http/node');
    } else {
      return await import('isomorphic-git/http/web');
    }
  })();

  /**
   * Creates or updates a bare git repository at the given gitdir by
   * ensuring a local bare repository exists and fetching the latest changes
   * from the remote URL.
   *
   * @remarks
   * The repository is configured to populate both `refs/remotes/origin/*` and `refs/heads/*`
   * on fetch, ensuring that local branches are always updated to match the remote branches.
   *
   * @param url - The remote repository URL.
   * @param gitdir - The path to the local git directory.
   * @param options - Additional options for cloning and fetching.
   *
   * @returns The resolved commit SHA of the fetched ref, which can be used for tracking the current state of the repository.
   */
  private static async initAndFetchBareRepo(
    url: URL | string,
    gitdir: Path,
    { corsProxy, onProgress }: KartCloneOptions = {}
  ) {
    const http = await this.http;
    const fs = this.throttledFs;
    if (url instanceof URL) {
      url = url.href;
    }

    const info = await getRemoteInfo({ http, corsProxy, url });
    const defaultBranch = info.HEAD?.replace('refs/heads/', '') || 'main';

    let repoExists = false;
    // check if the existing directory is a git repository with the same remote url
    if (gitdir.exists) {
      const remoteInfo = await listRemotes({ fs, gitdir: gitdir.absolute });
      const origin = remoteInfo.find((remote) => remote.remote === 'origin');
      const isSameRepo = origin?.url === url;

      if (!isSameRepo) {
        // delete existing directory
        await gitdir.rm({ recursive: true, force: true });
      } else {
        repoExists = true;
      }
    }

    // create a bare repository
    if (!repoExists) {
      console.log(`Cloning bare repository from ${url} into ${gitdir}...`);
      await init({ fs, gitdir: gitdir.absolute, bare: true, defaultBranch });
      await addRemote({ fs, gitdir: gitdir.absolute, remote: 'origin', url });
    } else {
      console.log(`Fetching latest changes from ${url} into existing repository at ${gitdir}...`);
    }

    // Configure git to populate the refs/remotes/origin/* AND refs/heads/* on fetch.
    // The ensures the list of local branches (refs/heads/*) is always updated to
    // match the remote branches when fetching.
    const currentOriginFetchConfigs = await getConfigAll({
      fs,
      gitdir: gitdir.absolute,
      path: 'remote.origin.fetch',
    });
    const REMOTE_FETCH_REF1 = '+refs/heads/*:refs/remotes/origin/*';
    const REMOTE_FETCH_REF2 = '+refs/heads/*:refs/heads/*';
    if (!currentOriginFetchConfigs.includes(REMOTE_FETCH_REF1)) {
      console.log(`Adding missing git config remote.origin.fetch "${REMOTE_FETCH_REF1}"`);
      await setConfig({
        fs,
        gitdir: gitdir.absolute,
        append: true,
        path: 'remote.origin.fetch',
        value: REMOTE_FETCH_REF1,
      });
    }
    if (!currentOriginFetchConfigs.includes(REMOTE_FETCH_REF2)) {
      console.log(`Adding missing git config remote.origin.fetch "${REMOTE_FETCH_REF2}"`);
      await setConfig({
        fs,
        gitdir: gitdir.absolute,
        append: true,
        path: 'remote.origin.fetch',
        value: REMOTE_FETCH_REF2,
      });
    }

    await gitFetch({
      fs,
      http,
      gitdir: gitdir.absolute,
      corsProxy,
      ref: defaultBranch,
      remoteRef: `refs/heads/${defaultBranch}`,
      prune: true, // remove non-existent branches
      onProgress,
    });
  }

  protected static inferRepoNameFromUrl(url: string | URL): string {
    if (url instanceof URL) {
      url = url.href;
    }

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

  /**
   * Discards the current index and replaces it with the contents at the given ref.
   *
   * @remarks
   * Use this to completely reset any uncommitted changes.
   *
   * @param gitDir - The path to the local git directory.
   * @param indexName - The name of the index to create (e.g. "index").
   * @param ref - The git ref to read the tree from (e.g. "HEAD").
   * @throws If the index cannot be discarded or replaced.
   * @returns A promise that resolves when the index has been successfully discarded and replaced.
   */
  private static async resetIndex(gitDir: Path, indexName: string, refObjectId: string) {
    const indexPath = gitDir.join('rooms', indexName, 'index');
    if (indexPath.exists) {
      await indexPath.rm({ recursive: true, force: true });
    }

    // populate the index with the contents of the tree at the given ref
    const repoTree = new GitTree(gitDir, gitDir, indexName, refObjectId);
    const index = await repoTree.getIndex();
    await walk({
      fs,
      gitdir: gitDir.absolute,
      trees: [TREE({ ref: repoTree.ref })],
      map: async (pathInRepo, [head]) => {
        if (!head) {
          return;
        }

        const type = await head?.type();

        // return the path to the tree ("folder") so that walk will recurse into it
        if (type === 'tree') {
          return pathInRepo || '.';
        }

        if (type === 'blob') {
          index.insert({
            filepath: pathInRepo,
            oid: await head.oid(),
            stats: await head.stat(),
          });
        }
      },
    });
    await repoTree.persistIndex();
  }

  async [Symbol.asyncDispose]() {
    this.data.removeAllEventListeners();

    // // Since a new index is created for each Kart instance to start working changes,
    // // we need to clean up the index when disposing the Kart instance.
    // const indexPath = this.repoDir.join('.kartjs', this.uuid);
    // if (indexPath.exists) {
    //   await indexPath.rm({ force: true });
    // }
  }

  dispose() {
    return this[Symbol.asyncDispose]();
  }
}
