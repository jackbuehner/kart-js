import './shims/loadShims.ts'; // this MUST be first to avoid @zenfs/core's incorrect Uint8Array polyfills

export { TableDatasetV3 } from './table-dataset-v3/TableDatasetV3.ts';
export { type TrackedChange } from './table-dataset-v3/WorkingFeatureCollection.ts';
export * from './utils/features/index.ts';

import { mount, resolveMountConfig } from '@zenfs/core';
import { IndexedDB } from '@zenfs/dom';
import { Kart as _Kart } from './Kart.ts';

/**
 * Initializes the file system that `kart-js` will use in the browser.
 *
 * You MUST call this function before using any other functionality from `kart-js` in a browser environment.
 *
 * @param storeName The name of the IndexedDB store to use.
 */
async function init(storeName: string) {
  // required by isomorphic-git
  window.Buffer = await import('buffer').then(({ Buffer }) => Buffer);

  // use indexdDB with ZenFS
  const fs = await resolveMountConfig({ backend: IndexedDB, storeName });
  mount(storeName, fs);
}

export class Kart extends _Kart {
  static async pull(...args: Parameters<typeof _Kart.pull>) {
    let [url, dir, ...restArgs] = args;

    // initialize an indexedDB store for ZenFS with the speicified or inferred directory as its name
    const storeName = dir ?? Kart.inferRepoNameFromUrl(url);
    await init(storeName);

    return _Kart.pull(url, dir, ...restArgs);
  }
}
