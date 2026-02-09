import 'isomorphic-git';
import type { GitIndexManager } from 'isomorphic-git/managers';

declare module 'isomorphic-git' {
  type GitIndexInstance = Parameters<Parameters<typeof GitIndexManager.acquire>[1]>[0];
  type GitIndexConstructor = {
    from(buffer: any): Promise<GitIndex>;
    fromBuffer(buffer: any): Promise<GitIndex>;
    _entryToBuffer(entry: any): Promise<any>;
    new (entries: any, unmergedPaths: any): GitIndex;
  };
  export const GitIndex: GitIndexConstructor;
  export type GitIndex = GitIndexInstance;

  type DirectoryStructureItem = {
    type: 'blob' | 'tree';
    fullpath: string;
    basename: string;
    metadata: unknown;
    parent?: DirectoryStructureItem;
    children: DirectoryStructureItem[];
  };

  export function __internal_flatFileListToDirectoryStructure(files: any): Map<string, DirectoryStructureItem>;

  export function __internal_normalizeStats(e: unknown): {
    ctimeSeconds: number;
    ctimeNanoseconds: number;
    mtimeSeconds: number;
    mtimeNanoseconds: number;
    dev: number;
    ino: number;
    mode: number;
    uid: number;
    gid: number;
    size: number;
  };

  export function __internal_mode2type(mode: number): 'commit' | 'blob' | 'tree';

  export const __internal_GitWalkSymbol: symbol;

  export function __internal_compareStrings(a: any, b: any): number;
}
