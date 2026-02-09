import type { Kart } from '../../Kart.ts';
import type { KartDiff } from '../../table-dataset-v3/diffs.js';

export class Diff {
  private core: Kart;

  constructor(core: Kart) {
    this.core = core;
  }

  /**
   * Generates a diff object representing the changes made to all datasets in the Kart repository.
   *
   * @returns A promise that resolves to a `KartDiff.HexWkB.v1.Diff` object containing the diffs for all datasets.
   */
  async toDiff() {
    const diffObject: KartDiff.HexWkB.v1.Diff = {};

    for await (const [name, value] of this.core.data) {
      const diff = await value.dataset.working.computeDiff();
      const datasetDiff = diff?.['kart.diff/v1+hexwkb'];
      if (datasetDiff) {
        diffObject[name] = datasetDiff[name];
      }
    }

    return {
      'kart.patch/v1': {
        base: this.core.repoTree.ref,
        crs: 'EPSG:4326',
      },
      'kart.diff/v1+hexwkb': diffObject,
    };
  }
}
