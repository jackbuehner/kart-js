export { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
export { WorkingFeatureCollection } from './table-dataset-v3/WorkingFeatureCollection.ts';

import { configureSingle, Passthrough, PassthroughFS, type PassthroughOptions } from '@zenfs/core';
import fs from 'node:fs';
import { Kart as _Kart } from './Kart.ts';
import { loadShims } from './shims/loadShims.ts';

loadShims();

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

  private encode(path: string) {
    return path.replaceAll(':', this.COLON_REPLACEMENT);
  }

  private decode(path: string) {
    return path.replaceAll(this.COLON_REPLACEMENT, ':');
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
