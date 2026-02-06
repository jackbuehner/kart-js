import './shims/loadShims.ts'; // this MUST be first to avoid @zenfs/core's incorrect Uint8Array polyfills

export { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
export { WorkingFeatureCollection } from './table-dataset-v3/WorkingFeatureCollection.ts';

import { configureSingle, Passthrough, PassthroughFS, type PassthroughOptions } from '@zenfs/core';
import { exec } from 'node:child_process';
import nodeFS from 'node:fs';
import { promisify } from 'node:util';
import { Kart as _Kart } from './Kart.ts';

// Windows does not allow case-sensitive files and directories by default.
// Since the git repositorys Kart works with may contain files or directories
// that only differ by case, we need to enable case-sensitivity on Windows.
// This wrapper around the NodeFS adds this functionality.
const fs = (() => {
  async function enableCaseSensitivity(path: string | Buffer | URL) {
    if (process.platform !== 'win32') return;
    try {
      console.log(`Enabling case sensitivity for path: ${path.toString()}`);
      return new Promise<void>((resolve, reject) => {
        exec(`fsutil.exe file setCaseSensitiveInfo "${path.toString()}" enable`, (error, stdout, stderr) => {
          if (error) {
            reject(error);
            return;
          }
          resolve();
        });
      });
    } catch (error) {
      throw new Error(
        `Failed to enable case sensitivity for path: ${path.toString()}. Reason: ${(error as Error).message}`
      );
    }
  }

  return {
    ...nodeFS,
    mkdir: Object.assign(
      (path: nodeFS.PathLike, ...args: any[]) => {
        const callback = args[args.length - 1];
        if (typeof callback === 'function') {
          const wrappedCallback = (err: Error | null, pathCreated?: string) => {
            if (!err) {
              enableCaseSensitivity(path).finally(() => callback(err, pathCreated));
            } else {
              callback(err, pathCreated);
            }
          };
          args[args.length - 1] = wrappedCallback;
        }
        return (nodeFS.mkdir as any)(path, ...args);
      },
      {
        __promisify__: async (path: nodeFS.PathLike, options?: nodeFS.MakeDirectoryOptions) => {
          const result = await promisify(fs.mkdir)(path, options);
          await enableCaseSensitivity(path);
          return result;
        },
      }
    ),
    promises: {
      ...nodeFS.promises,
      mkdir: async (path: nodeFS.PathLike, options?: nodeFS.MakeDirectoryOptions) => {
        const result = await nodeFS.promises.mkdir(path, options);
        await enableCaseSensitivity(path);
        return result;
      },
    },
  } as typeof nodeFS;
})();

let hasInitialized = false;
async function init() {
  const workingDir = process.cwd().replaceAll('\\', '/') + '/tmp';
  if (!(await fs.promises.stat(workingDir).catch(() => false))) {
    await fs.promises.mkdir(workingDir, { recursive: true });
  }

  // use NodeFS with ZenFS
  await configureSingle({
    backend: EncodedSpecialCharacters as Passthrough,
    fs,
    prefix: workingDir,
  });

  hasInitialized = true;
}

export class Kart extends _Kart {
  static async pull(...args: Parameters<typeof _Kart.pull>) {
    if (!hasInitialized) {
      await init();
    }
    return _Kart.pull(...args);
  }
}

/**
 * A variant of PassthroughFS that encodes characters that
 * are invalid on Windows but valid on other operating systems.
 *
 * Currently, only the colon (:) character is encoded.
 *
 * When a file is written to disk, the special characters are
 * replaced with safe alternatives. When a file is read from disk,
 * the safe alternatives are replaced back to the original characters.
 * From the perspective of the consumer of the FS, the paths appear unchanged.
 */
class EncodedSpecialCharactersFS extends PassthroughFS {
  private readonly COLON_REPLACEMENT = '__COLON__';
  private readonly QUOTE_REPLACEMENT = '__QUOTE__';
  private readonly ASTERISK_REPLACEMENT = '__ASTERISK__';
  private readonly QUESTION_REPLACEMENT = '__QUESTION__';
  private readonly LESS_THAN_REPLACEMENT = '__LESS_THAN__';
  private readonly GREATER_THAN_REPLACEMENT = '__GREATER_THAN__';
  private readonly PIPE_REPLACEMENT = '__PIPE__';

  private encode(path: string) {
    return path
      .replaceAll(':', this.COLON_REPLACEMENT)
      .replaceAll('"', this.QUOTE_REPLACEMENT)
      .replaceAll('*', this.ASTERISK_REPLACEMENT)
      .replaceAll('?', this.QUESTION_REPLACEMENT)
      .replaceAll('<', this.LESS_THAN_REPLACEMENT)
      .replaceAll('>', this.GREATER_THAN_REPLACEMENT)
      .replaceAll('|', this.PIPE_REPLACEMENT);
  }

  private decode(path: string) {
    return path
      .replaceAll(this.COLON_REPLACEMENT, ':')
      .replaceAll(this.QUOTE_REPLACEMENT, '"')
      .replaceAll(this.ASTERISK_REPLACEMENT, '*')
      .replaceAll(this.QUESTION_REPLACEMENT, '?')
      .replaceAll(this.LESS_THAN_REPLACEMENT, '<')
      .replaceAll(this.GREATER_THAN_REPLACEMENT, '>')
      .replaceAll(this.PIPE_REPLACEMENT, '|');
  }

  override path(path: string): string {
    return super.path(this.encode(path));
  }

  override async readdir(path: string): Promise<string[]> {
    const entries = await super.readdir(path);
    return entries.map((name) => this.decode(name));
  }

  override readdirSync(path: string): string[] {
    const entries = super.readdirSync(path);
    return entries.map((name) => this.decode(name));
  }
}

const EncodedSpecialCharacters = {
  name: 'EncodedSpecialCharacters',
  options: {
    fs: { type: 'object', required: true },
    prefix: { type: 'string', required: true },
  },
  create({ fs, prefix }: PassthroughOptions) {
    return new EncodedSpecialCharactersFS(fs, prefix);
  },
};
