export { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
export { WorkingFeatureCollection } from './table-dataset-v3/WorkingFeatureCollection.ts';

import { configureSingle, Passthrough } from '@zenfs/core';
import fs from 'node:fs';
import { Kart } from './Kart.ts';

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
}
init();

export { Kart };
