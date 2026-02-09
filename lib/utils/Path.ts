import fs, { existsSync, readdirSync, readFileSync, realpathSync, statSync } from '@zenfs/core';
import * as path from '@zenfs/core/path';
import { mkdir, opendir, readdir, rm, writeFile } from '@zenfs/core/promises';
import {
  __internal_compareStrings,
  __internal_flatFileListToDirectoryStructure,
  __internal_GitWalkSymbol,
  __internal_mode2type,
  __internal_normalizeStats,
  GitIndex,
  readBlob,
  walk,
  type WalkerEntry,
  type WalkerMap,
} from 'isomorphic-git';
import pLimit from 'p-limit';
import { FileNotFoundError, FileReadError } from './errors.ts';

export class VirtualPath<PathType extends VirtualPath<any> = VirtualPath<any>> {
  protected fullPath: string;

  constructor(fullPath: string) {
    this.fullPath = fullPath.replaceAll('\\', '/');
  }

  /**
   * Joins the given segments into a single path and returns a new Path instance.
   */
  static join(...segments: string[]) {
    const joinedPath = segments.join('/').replaceAll('//', '/');
    return new VirtualPath(joinedPath);
  }

  /**
   * Joins the given segments to the current path and returns a new Path instance.
   */
  join(...segments: string[]) {
    const joinedPath = [this.fullPath, ...segments].join('/').replaceAll('//', '/');
    const Constructor = this.constructor as new (path: string) => PathType;
    return new Constructor(joinedPath);
  }

  get absolute() {
    try {
      return realpathSync(this.fullPath).replaceAll('\\', '/');
    } catch (error) {
      const exposedError = new FileNotFoundError(`Invalid path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Extracts the file name without the extension from the full path.
   */
  get basename(): string {
    const dirParts = this.fullPath.split('/');
    const fileName = dirParts[dirParts.length - 1];
    if (!fileName) return '';
    const dotIndex = fileName.lastIndexOf('.');
    return dotIndex === -1 ? fileName : fileName.substring(0, dotIndex);
  }

  get name(): string {
    const dirParts = this.fullPath.split('/');
    return dirParts[dirParts.length - 1] ?? '';
  }

  get parentPath(): PathType | null {
    const dirParts = this.fullPath.split('/');
    const parentParts = dirParts.slice(0, dirParts.length - 1);
    if (parentParts.length === 0) {
      return null;
    }
    const Constructor = this.constructor as new (path: string) => PathType;
    return new Constructor(parentParts.join('/'));
  }

  /**
   * Extracts the file extension from the full path.
   */
  get extension(): string {
    const dirParts = this.fullPath.split('/');
    const fileName = dirParts[dirParts.length - 1];
    if (!fileName) return '';
    const dotIndex = fileName.lastIndexOf('.');
    return dotIndex === -1 ? '' : fileName.substring(dotIndex + 1);
  }

  toString() {
    return this.fullPath;
  }

  toJSON() {
    return this.fullPath;
  }

  /**
   * Gets a relative path string from this path to the given path.
   */
  relativeTo(to: VirtualPath<PathType>): string {
    return path.relative(this.absolute, to.absolute).replaceAll('\\', '/');
  }
}

export class Path extends VirtualPath<Path> {
  constructor(fullPath: string) {
    super(fullPath);
  }

  get exists() {
    try {
      return existsSync(this.fullPath);
    } catch {
      return false;
    }
  }

  get isFile() {
    return statSync(this.fullPath).isFile();
  }

  get isDirectory() {
    return statSync(this.fullPath).isDirectory();
  }

  /**
   * Reads the file and returns its contents as a string.
   * @throws {FileReadError} If the file cannot be read.
   */
  readFileSync(options: { encoding: import('fs').EncodingOption; flag?: string }): string;
  /**
   * Reads the file and returns its contents as a Uint8Array.
   * @throws {FileReadError} If the file cannot be read.
   * @throws {FileNotFoundError} If the file does not exist.
   */
  readFileSync(options?: { flag?: string }): Uint8Array;
  readFileSync(options?: { encoding?: import('fs').EncodingOption; flag?: string }): string | Uint8Array {
    if (!this.exists) {
      throw new FileNotFoundError(`File does not exist at path: ${this.fullPath}`);
    }

    if (!this.isFile) {
      throw new FileReadError(`Path is not a file: ${this.fullPath}`);
    }

    try {
      if (options?.encoding) {
        return readFileSync(this.fullPath, options) as unknown as string;
      }
      const buffer = readFileSync(this.fullPath, options);
      return new Uint8Array(buffer).slice();
    } catch (error) {
      const exposedError = new FileReadError(`Failed to read file at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Reads the directory and returns an array of Path instances for each entry.
   * @throws {FileNotFoundError} If the directory does not exist.
   * @throws {FileReadError} If the directory cannot be read.
   */
  readDirectorySync(options?: { recursive?: boolean; encoding?: BufferEncoding }): Path[] {
    if (!this.exists) {
      throw new FileNotFoundError(`Directory does not exist at path: ${this.fullPath}`);
    }

    if (!this.isDirectory) {
      throw new FileReadError(`Path is not a directory: ${this.fullPath}`);
    }

    try {
      return readdirSync(this.fullPath, { recursive: options?.recursive, encoding: 'utf-8' }).map((name) =>
        this.join(name)
      );
    } catch (error) {
      const exposedError = new FileReadError(`Failed to read directory at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Reads the directory and returns an array of Path instances for each entry.
   * @throws {FileNotFoundError} If the directory does not exist.
   * @throws {FileReadError} If the directory cannot be read.
   */
  async readDirectory(options?: { recursive?: boolean; encoding?: BufferEncoding }): Promise<Path[]> {
    if (!this.exists) {
      throw new FileNotFoundError(`Directory does not exist at path: ${this.fullPath}`);
    }

    if (!this.isDirectory) {
      throw new FileReadError(`Path is not a directory: ${this.fullPath}`);
    }

    try {
      return await readdir(this.fullPath, { recursive: options?.recursive, encoding: 'utf-8' }).then(
        (names) => {
          return names.map((name) => this.join(name));
        }
      );
    } catch (error) {
      const exposedError = new FileReadError(`Failed to read directory at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Reads the directory and yields a Path instance for each directory entry.
   * @throws {FileNotFoundError} If the directory does not exist.
   * @throws {FileReadError} If an error occurs while reading an entry in the directory.
   */
  async *openDirectory(): AsyncGenerator<Path, void, void> {
    if (!this.exists) {
      throw new FileNotFoundError(`Directory does not exist at path: ${this.fullPath}`);
    }

    if (!this.isDirectory) {
      throw new FileReadError(`Path is not a directory: ${this.fullPath}`);
    }

    try {
      // TODO: once @zenfs/core supports opendir with options, allow passing options here
      const dir = await opendir(this.fullPath);
      for await (const dirent of dir) {
        yield this.join(dirent.name);
      }
    } catch (error) {
      const exposedError = new FileReadError(`Failed to read item in directory at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * Removes the file or directory at this path.
   */
  async rm(options?: { recursive?: boolean; force?: boolean }) {
    if (!this.exists) {
      return;
    }
    return await rm(this.fullPath, options);
  }

  /**
   * Writes data to a file at this path, replacing the file if it already exists. If the file does not exist, creates a new file.
   *
   * @throws {FileReadError} If the file cannot be written.
   */
  async writeFile(
    data: string | Uint8Array,
    options?: {
      encoding?: import('fs').ObjectEncodingOptions['encoding'];
      flag?: import('fs').OpenMode;
      flush?: boolean;
      mode?: import('fs').Mode;
    }
  ) {
    try {
      await writeFile(this.fullPath, data, options);
    } catch (error) {
      const exposedError = new FileReadError(`Failed to write file at path: ${this.fullPath}`);
      exposedError.cause = error;
      throw exposedError;
    }
  }

  /**
   * If the directory does not exist, creates a directory at this path. If the directory already exists, does nothing.
   * @throws {FileReadError} If a file already exists at this path, or if the directory cannot be created.
   */
  async makeDirectory(options?: { recursive?: boolean; mode?: import('fs').Mode }) {
    if (!this.exists) {
      try {
        await mkdir(this.fullPath, options);
      } catch (error) {
        const exposedError = new FileReadError(`Failed to create directory at path: ${this.fullPath}`);
        exposedError.cause = error;
        throw exposedError;
      }
    }
  }
}

export class GitTree extends VirtualPath<GitTree> {
  private repoDir: VirtualPath;
  readonly indexName: string;
  readonly ref: string;
  private gitCache: {};

  /**
   * Creates a GitTree instance representing a path within a Git repository at a specific ref.
   *
   * @param fullPath The full path within the Git repository, prefixed by the path to the git respository.
   * @param repoDir The root directory of the Git repository's git directory (i.e. .git folder).
   * @param ref - The Git ref (branch, tag, or commit SHA) to from where to read the tree.
   */
  constructor(
    fullPath: string | VirtualPath,
    repoDir: VirtualPath,
    indexName: string,
    ref: string = 'HEAD',
    gitCache: {} = {}
  ) {
    super(typeof fullPath === 'string' ? fullPath : fullPath.absolute);
    this.repoDir = repoDir;
    this.indexName = indexName;
    this.ref = ref;
    this.gitCache = gitCache;
  }

  join(...segments: string[]) {
    const joinedPath = super.join(...segments).fullPath;
    return new GitTree(joinedPath, this.repoDir, this.indexName, this.ref, this.gitCache);
  }

  get parentPath(): GitTree | null {
    const parentPath = super.parentPath;
    if (parentPath === null) {
      return null;
    }
    return new GitTree(parentPath, this.repoDir, this.indexName, this.ref, this.gitCache);
  }

  get pathInsideRepo(): string {
    return this.repoDir.relativeTo(this);
  }

  private static _indexCache = new Map<string, GitIndex>();

  /**
   * Gets the Git index for this GitTree's repository and index name, either
   * from cache or by reading and parsing the index file.
   */
  async getIndex(): Promise<GitIndex> {
    if (GitTree._indexCache.has(this.indexName)) {
      return GitTree._indexCache.get(this.indexName)!;
    }

    const indexPath = new Path(this.repoDir.join(this.indexName).absolute);
    if (indexPath.exists) {
      const indexBuffer = await fs.promises.readFile(indexPath.absolute);
      const index = await GitIndex.fromBuffer(indexBuffer);
      GitTree._indexCache.set(this.indexName, index);
      return index;
    }

    const newIndex = new GitIndex(null, null);
    GitTree._indexCache.set(this.indexName, newIndex);
    return newIndex;
  }

  /**
   * Persists the given GitIndex to disk at the location of this GitTree's
   * repository and index name.
   */
  async persistIndex() {
    const indexPath = path.join(this.repoDir.absolute, this.indexName);
    const index = await this.getIndex();
    const buffer = (await index.toObject()) as Buffer;
    await writeFile(indexPath, buffer);
  }

  /**
   * A Walker that is compatable with isomorphic-git's walk function.
   */
  private INDEX() {
    const _treePromise = this.getIndex().then((index) => {
      return __internal_flatFileListToDirectoryStructure(index.entries);
    });
    const gitdir = this.repoDir.absolute;
    const cache = this.gitCache;

    class IndexWalker {
      treePromise = _treePromise;
      ConstructEntry: any;

      constructor() {
        const walker = this;
        this.ConstructEntry = class TreeEntry {
          _fullpath: string;
          _type?: ReturnType<typeof __internal_mode2type>;
          _mode?: number;
          _oid?: string;

          constructor(fullpath: string) {
            this._fullpath = fullpath;
          }

          async type() {
            if (this._type === undefined) {
              await this.stat();
            }
            return this._type!;
          }

          async mode() {
            if (this._mode === undefined) {
              await this.stat();
            }
            return this._mode!;
          }

          async stat() {
            const tree = await walker.treePromise;
            const inode = tree.get(this._fullpath);
            if (!inode) {
              throw new Error(`ENOENT: no such file or directory, lstat '${this._fullpath}'`);
            }
            const stats = inode.type === 'tree' ? undefined : __internal_normalizeStats(inode.metadata);
            this._type = !stats ? 'tree' : __internal_mode2type(stats.mode);
            this._mode = stats?.mode;
            return stats!;
          }

          async content() {
            try {
              const result = await readBlob({
                fs,
                gitdir,
                oid: await this.oid(),
                cache,
              });
              return result.blob;
            } catch (error) {
              return undefined;
            }
          }

          async oid() {
            if (this._oid) {
              return this._oid;
            }

            const tree = await walker.treePromise;
            const inode = tree.get(this._fullpath);
            if (
              !inode ||
              !inode.metadata ||
              typeof inode.metadata !== 'object' ||
              !('oid' in inode.metadata) ||
              typeof inode.metadata.oid !== 'string'
            ) {
              throw new Error(
                `Failed to get oid for path '${this._fullpath}' in index. Entry not found or missing oid.`
              );
            }
            this._oid = inode.metadata.oid;
            return this._oid;
          }
        };
      }

      async readdir(entry: WalkerEntry & { _fullpath: string }) {
        const filepath = entry._fullpath;
        const tree = await _treePromise;
        const inode = tree.get(filepath);
        if (!inode) return null;
        if (inode.type === 'blob') return null;
        if (inode.type !== 'tree') {
          throw new Error(`ENOTDIR: not a directory, scandir '${filepath}'`);
        }
        const names = inode.children.map((inode) => inode.fullpath);
        names.sort(__internal_compareStrings);
        return names;
      }
    }

    return {
      Symbol: __internal_GitWalkSymbol,
      [__internal_GitWalkSymbol]: () => new IndexWalker(),
    };
  }

  /**
   * Recursively reads the directory (tree) for the current index,
   * yielding each file (blob) found.
   * @param concurrentLimit - Optional limit on the number of concurrent file reads.
   * @param skipReadContents - If true, the yielded content function will always return undefined.
   */
  async *walkDirectory(
    concurrentLimit?: number,
    skipReadContents: boolean = false
  ): AsyncGenerator<[GitTree, Uint8Array<ArrayBufferLike> | null | undefined], void, void> {
    const queue: [GitTree, Promise<Uint8Array<ArrayBufferLike> | null> | undefined][] = [];

    let wakeYieldLoop: () => void;
    let signal = new Promise<void>((resolve) => {
      wakeYieldLoop = resolve;
    });

    let walkDone = false;

    const limit = concurrentLimit ? pLimit(concurrentLimit) : null;
    const map: WalkerMap = async (filepath, [index]) => {
      if (!index) {
        return null;
      }

      const type = await index.type();
      if (type !== 'blob' && type !== 'tree') {
        return null;
      }

      // never walk into a directory that is not related to
      // our target directory
      const isAncestor = this.pathInsideRepo.startsWith(filepath) || filepath === '.';
      const isSameOrDescendant = filepath.startsWith(this.pathInsideRepo);
      const shouldWalk = isAncestor || isSameOrDescendant;
      if (!shouldWalk) {
        return null;
      }

      // return the path to the tree ("folder") so that walk will recurse into it
      if (type === 'tree') {
        return filepath || '.';
      }

      // add each blob that has content to the queue to be yielded
      if (type === 'blob' && isSameOrDescendant) {
        const gitPath = new GitTree(this.repoDir.join(filepath), this.repoDir, this.indexName, this.ref);
        let contentPromise: Promise<Uint8Array | null> | undefined = undefined;
        if (!skipReadContents) {
          contentPromise = limit
            ? limit(() => index.content().then((content) => content ?? null))
            : index.content().then((content) => content ?? null);
        }
        queue.push([gitPath, contentPromise]);
        wakeYieldLoop();
        signal = new Promise<void>((resolve) => {
          wakeYieldLoop = resolve;
        });

        return filepath;
      }
    };

    const walking = walk({
      fs,
      gitdir: this.repoDir.absolute,
      trees: [this.INDEX()],
      map,
      cache: this.gitCache,
    })
      .catch((error) => {
        throw new Error(`Failed to walk Git blobs at path ${this.fullPath}: ${error.message}`, {
          cause: error,
        });
      })
      .finally(() => {
        walkDone = true;
        wakeYieldLoop();
      });

    // stream results from the queue as they are populated
    while (!walkDone || queue.length > 0) {
      if (queue.length > 0) {
        const nextItem = queue.shift();
        if (nextItem) {
          const [nextGitPath, contentPromise] = nextItem;
          const content = await contentPromise;
          yield [nextGitPath, content];
        }
      } else {
        await signal;
      }
    }

    await walking; // Ensure any errors in walking are thrown
  }

  /**
   * Reads the contents of the directory (tree) at the given ref.
   *
   * To recursively read the directory, use `walkDirectory` instead.
   */
  async readDirectory() {
    return (await walk({
      fs,
      gitdir: this.repoDir.absolute,
      trees: [this.INDEX()],
      map: async (pathInRepo, [entry]) => {
        if (!entry) {
          return null;
        }

        const type = await entry.type();
        if (type !== 'blob' && type !== 'tree') {
          return null;
        }

        // never walk into a directory or examine a file
        // that is not a direct child of our target directory
        const isAncestor = this.pathInsideRepo.startsWith(pathInRepo) || pathInRepo === '.';
        const isDirectChild = pathInRepo.split('/').length === this.pathInsideRepo.split('/').length + 1;
        const shouldWalk = isAncestor || isDirectChild;
        if (!shouldWalk) {
          return null;
        }

        if (isDirectChild) {
          const vPath = new VirtualPath(pathInRepo);
          return {
            name: vPath.name,
            basename: vPath.basename,
            extension: vPath.extension,
            isFile: type === 'blob',
            isDirectory: type === 'tree',
            oid: entry.oid.bind(entry),
            content: entry.content.bind(entry),
          } as GitDirectoryEntry;
        }

        // since we never return the path to a tree once we reach our desintation
        // directory, this walk will not recurse into any further subdirectories
      },
      cache: this.gitCache,
    })) as Promise<GitDirectoryEntry[]>;
  }

  /**
   * Reads the file and returns its contents as a string.
   * @throws {FileNotFoundError} If the file does not exist at the specified path and ref.
   */
  async readFile(options: { encoding: string }): Promise<string>;
  /**
   * Reads the file and returns its contents as a Uint8Array.
   * @throws {FileNotFoundError} If the file does not exist at the specified path and ref.
   */
  async readFile(options?: {}): Promise<Uint8Array>;
  async readFile(options?: { encoding?: string }): Promise<Uint8Array | string> {
    if (!(await this.isFile)) {
      throw new FileNotFoundError(
        `NOFIL: File does not exist at path: ${this.fullPath} (index: ${this.indexName})`
      );
    }

    const entry = (await this.getIndex()).entriesMap.get(this.pathInsideRepo.normalize('NFC'));
    if (!entry) {
      throw new FileNotFoundError(
        `NOOID: File does not exist at path: ${this.fullPath} (index: ${this.indexName})`
      );
    }

    try {
      const result = await readBlob({
        fs,
        gitdir: this.repoDir.absolute,
        oid: entry.oid,
        cache: this.gitCache,
      });

      if (options?.encoding) {
        return new TextDecoder(options.encoding).decode(result.blob);
      }

      return result.blob;
    } catch (error) {
      throw new FileNotFoundError(
        `UKERR: File does not exist at path: ${this.fullPath} (index: ${this.indexName})`
      );
    }
  }

  private async getType() {
    const index = await this.getIndex();

    // BLOB: Directly check the entry in the index, which does not include any trees.
    const entry = index.entriesMap.get(this.pathInsideRepo.normalize('NFC'));
    if (entry) {
      const isBlob =
        entry.mode === GIT_MODE_BLOB ||
        entry.mode === GIT_MODE_EXECUTABLE_BLOB ||
        entry.mode === GIT_MODE_SYMLINK;
      if (isBlob) {
        return 'blob';
      }

      const isCommit = entry.mode === GIT_MODE_COMMIT;
      if (isCommit) {
        return 'commit';
      }
    }

    // TREE: Check if the path is a prefix of any entry in the index, which would
    //       indicate that it is a directory (tree) containing those entries.
    const isTree = (await this.readDirectory()).length > 0;
    if (isTree) {
      return 'tree';
    }

    return null;
  }

  get exists() {
    return this.getType().then((type) => type === 'blob' || type === 'tree');
  }

  get isFile() {
    return this.getType().then((type) => type === 'blob');
  }

  get isDirectory() {
    return this.getType().then((type) => type === 'tree');
  }

  /**
   * Removes this file or directory from the git index.
   *
   * @remarks
   * The git index is a record of changes that can be committed.
   * Removing a file or directory from the index does not delete it
   * until a commit is made.
   *
   * If the path is a directory, all files within the directory
   * will be removed from the index.
   *
   * @throws {FileNotFoundError} If the file or directory does not exist at the specified path and ref.
   */
  async rm() {
    const type = await this.getType();
    const isDirectory = type === 'tree';
    const isFile = type === 'blob';

    const index = await this.getIndex();

    if (isDirectory) {
      const filepathsToDelete: string[] = [];

      for await (const [filepath] of this.walkDirectory()) {
        filepathsToDelete.push(filepath.pathInsideRepo);
      }

      for (const filepath of filepathsToDelete) {
        index.delete({ filepath });
      }

      return this.persistIndex();
    }

    if (isFile) {
      index.delete({ filepath: this.pathInsideRepo });
      return this.persistIndex();
    }

    throw new FileNotFoundError(
      `Path does not exist in the repository or is not a file or directory: ${this.fullPath}`
    );
  }
}

const GIT_MODE_BLOB = 0o100644;
const GIT_MODE_EXECUTABLE_BLOB = 0o100755;
const GIT_MODE_SYMLINK = 0o120000;
const GIT_MODE_TREE = 0o040000;
const GIT_MODE_COMMIT = 0o160000;

interface GitDirectoryEntry {
  name: string;
  basename: string;
  extension: string;
  isFile: boolean;
  isDirectory: boolean;
  oid: () => Promise<string>;
  content: () => Promise<void | Uint8Array>;
}
