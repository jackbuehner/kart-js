import type { KartFeatureCollection } from '../utils/features/index.ts';
import { Path } from '../utils/index.ts';
import { Features } from './Feature.ts';
import { Legend, Legends } from './Legend.ts';
import { PathStructure } from './PathStructure.ts';
import { RawFeature, RawFeatures } from './RawFeature.ts';
import { Schema } from './Schema.ts';
import { makeSerializeable } from './serializer.ts';
import { WorkingFeatureCollection } from './WorkingFeatureCollection.ts';

export class TableDatasetV3 {
  #datasetPath: Path;

  readonly type = 'table-dataset-v3';
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  readonly pathStructure: PathStructure;
  readonly schema: Schema;
  readonly legends: Legends;

  readonly working: WorkingFeatureCollection;

  constructor(repoPath: Path, id: string) {
    if (!TableDatasetV3.isValidDataset(repoPath, id)) {
      throw new Error(`Dataset with id "${id}" does not exist or is not a valid table dataset v3.`);
    }

    this.#datasetPath = repoPath.join(id);
    this.id = id;

    try {
      const validatedContents = TableDatasetV3.getValidatedContents(repoPath, id);
      if (!validatedContents) {
        throw new Error('Could not validate dataset contents.');
      }
      this.title = validatedContents.title;
      this.pathStructure = validatedContents.pathStructure;
      this.schema = validatedContents.schema;
      this.description = validatedContents.description;
      this.legends = validatedContents.legends;
    } catch (error) {
      const toThrow = new Error(`Dataset with id "${id}" has invalid contents: ${(error as Error).message}`);
      if (error instanceof Error) {
        toThrow.stack = error.stack;
        toThrow.cause = error.cause;
        toThrow.name = error.name;
      }
      throw toThrow;
    }

    this.working = new WorkingFeatureCollection(this.id, this.toFeatureCollection(), this.schema);
  }

  static isValidDataset(repoDir: Path, id: string, validateContents = false) {
    const folderExists = repoDir.readDirectorySync().findIndex((item) => item.name === id) !== -1;
    if (!folderExists) {
      return false;
    }

    // table datasets MUST have a .table-dataset folder inside their root folder that contains at least the feature and meta folders
    const tableDatasetPath = repoDir.join(id, '.table-dataset');
    if (!tableDatasetPath.exists) {
      return false;
    }

    const tableDatasetContents = tableDatasetPath.readDirectorySync();
    const hasMetaFolder = tableDatasetContents.findIndex((item) => item.name === 'meta') !== -1;
    if (!hasMetaFolder) {
      return false;
    }

    const metaFolderContents = tableDatasetPath.join('meta').readDirectorySync();
    const hasTitleFile = metaFolderContents.findIndex((file) => file.name === 'title') !== -1;
    const hasSchemaFile = metaFolderContents.findIndex((file) => file.name === 'schema.json') !== -1;
    const hasPathStructureFile =
      metaFolderContents.findIndex((file) => file.name === 'path-structure.json') !== -1;
    if (!hasTitleFile || !hasSchemaFile || !hasPathStructureFile) {
      return false;
    }

    const hasLegendFolder = metaFolderContents.findIndex((file) => file.name === 'legend') !== -1;
    if (!hasLegendFolder) {
      return false;
    }
    const hasAtLeastOneLegendFile = tableDatasetPath.join('meta', 'legend').readDirectorySync().length > 0;
    if (!hasAtLeastOneLegendFile) {
      return false;
    }

    if (!validateContents) {
      return true;
    }

    try {
      this.getValidatedContents(repoDir, id);
      return true;
    } catch {
      return false;
    }
  }

  private static getValidatedContents(repoDir: Path, id: string) {
    if (!TableDatasetV3.isValidDataset(repoDir, id, false)) {
      return;
    }

    const titleFilePath = repoDir.join(id, '.table-dataset', 'meta', 'title');
    const title = titleFilePath.readFileSync({ encoding: 'utf-8' }).trim();

    const descriptionFilePath = repoDir.join(id, '.table-dataset', 'meta', 'description');
    let description: string | undefined = undefined;
    if (descriptionFilePath.exists && descriptionFilePath.isFile) {
      description = descriptionFilePath.readFileSync({ encoding: 'utf-8' }).trim();
    }

    const pathStructurePath = repoDir.join(id, '.table-dataset', 'meta', 'path-structure.json');
    const pathStructure = PathStructure.fromFile(pathStructurePath);

    const schemaFilePath = repoDir.join(id, '.table-dataset', 'meta', 'schema.json');
    const schema = Schema.fromFile(schemaFilePath);

    const legendDirPath = repoDir.join(id, '.table-dataset', 'meta', 'legend');
    const legendFiles = legendDirPath.readDirectorySync();
    const legends = new Legends();
    for (const legendFile of legendFiles) {
      const legend = Legend.fromFile(legendFile);
      legends.add(legend);
    }

    return {
      title,
      description,
      pathStructure,
      schema,
      legends,
    };
  }

  /**
   * Gets each feature inside the feature folder for the dataset in its raw form.
   */
  protected toRawFeatures() {
    const rawFeatures = new RawFeatures();

    const featureDirPath = this.#datasetPath.join('.table-dataset', 'feature');
    if (!featureDirPath.exists) {
      return rawFeatures; // a missing folder indicates no features
    }

    const featureFiles = featureDirPath.readDirectorySync({ recursive: true }).filter((file) => file.isFile);

    for (const file of featureFiles) {
      const rawFeature = RawFeature.fromFile(file);
      rawFeatures.add(rawFeature);
    }

    return rawFeatures;
  }

  /**
   * Gets each feature inside the feature folder for the dataset.
   */
  protected toFeatures() {
    const features = new Features();

    for (const rawFeature of this.toRawFeatures()) {
      const feature = rawFeature.toFeature(this.schema, this.legends, this.pathStructure);
      features.add(feature);
    }

    return features;
  }

  toFeatureCollection() {
    return makeSerializeable({
      type: 'FeatureCollection',
      features: this.toFeatures()
        .map((feature) => feature.toGeoJSON())
        .filter((x) => !!x),
    } satisfies KartFeatureCollection);
  }

  toGeoJSON() {
    return this.toFeatureCollection();
  }
}
