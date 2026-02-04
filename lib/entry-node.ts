export { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
export { WorkingFeatureCollection } from './table-dataset-v3/WorkingFeatureCollection.ts';

import { configureSingle, Passthrough } from '@zenfs/core';
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
    backend: Passthrough,
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
