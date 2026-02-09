import Flatbush from 'flatbush';
import type { Kart } from '../Kart.ts';
import { GitTree } from '../utils/Path.ts';
import type { KartFeatureCollection } from '../utils/features/index.ts';
import { Path } from '../utils/index.ts';
import { CRS, CRSs } from './CRS.ts';
import { Feature, Features } from './Feature.ts';
import { Legend, Legends } from './Legend.ts';
import { PathStructure } from './PathStructure.ts';
import { RawFeature, RawFeatures } from './RawFeature.ts';
import { Schema } from './Schema.ts';
import { WorkingFeatureCollection } from './WorkingFeatureCollection.ts';
import serializer from './serializer.ts';

export class TableDatasetV3 {
  readonly tree: GitTree;
  readonly generatedPath: Path;

  readonly type = 'table-dataset-v3';
  readonly id: string;
  readonly title: string;
  readonly description?: string;
  readonly pathStructure: PathStructure;
  readonly schema: Schema;
  readonly legends: Legends;
  readonly crss: CRSs;

  /**
   * The total number of features in the dataset.
   *
   * This is a pre-computed value stored when the dataset is created.
   * It is based on the number of files in the feature directory.
   *
   * For the current numer of features, use `working.length` instead.
   */
  readonly featureCount;
  readonly working: WorkingFeatureCollection;

  private cache: TableDatasetV3Cache = {};

  private constructor(
    id: string,
    title: string,
    tree: GitTree,
    generatedPath: Path,
    pathStructure: PathStructure,
    schema: Schema,
    description: string | undefined,
    legends: Legends,
    crss: CRSs,
    featureCount: number
  ) {
    this.tree = tree;
    this.id = id;
    this.generatedPath = generatedPath;
    this.title = title;
    this.pathStructure = pathStructure;
    this.schema = schema;
    this.description = description;
    this.legends = legends;
    this.crss = crss;
    this.featureCount = featureCount;
    this.working = new WorkingFeatureCollection(this);
  }

  static async create(core: Kart, id: string): Promise<TableDatasetV3> {
    // this is where files for this dataset that are generated and not version-controlled are stored
    const generatedPath = core.repoDir.join(core.repoTree.ref, id);
    if (!generatedPath.exists) {
      generatedPath.makeDirectory({ recursive: true });
    }

    const tree = core.repoTree.join(id);
    const featureDirectoryTree = tree.join('.table-dataset', 'feature');

    try {
      const validatedContents = await TableDatasetV3.getValidatedContents(core.repoTree, id);
      if (!validatedContents) {
        throw new Error(`Dataset with id "${id}" does not exist or is not a valid table dataset v3.`);
      }

      const featureCount = await this.getFeatureCount(featureDirectoryTree);

      return new TableDatasetV3(
        id,
        validatedContents.title,
        tree,
        generatedPath,
        validatedContents.pathStructure,
        validatedContents.schema,
        validatedContents.description,
        validatedContents.legends,
        validatedContents.crss,
        featureCount
      );
    } catch (error) {
      const toThrow = new Error(`Dataset with id "${id}" has invalid contents: ${(error as Error).message}`);
      if (error instanceof Error) {
        toThrow.stack = error.stack;
        toThrow.cause = error.cause;
        toThrow.name = error.name;
      }
      throw toThrow;
    }
  }

  static async isValidDataset(repoTree: GitTree, id: string, validateContents = false) {
    const datasetTree = repoTree.join(id);
    if (!(await datasetTree.exists)) {
      return false;
    }

    // table datasets MUST have a .table-dataset folder inside their root folder that contains at least the feature and meta folders
    const tableDatasetPath = datasetTree.join('.table-dataset');
    if (!(await tableDatasetPath.exists)) {
      return false;
    }

    const tableDatasetContents = await tableDatasetPath.readDirectory();
    const hasMetaFolder = tableDatasetContents.findIndex((item) => item.name === 'meta') !== -1;
    if (!hasMetaFolder) {
      return false;
    }

    const metaFolderContents = await tableDatasetPath.join('meta').readDirectory();
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
    const hasAtLeastOneLegendFile = (await tableDatasetPath.join('meta', 'legend').readDirectory()).length > 0;
    if (!hasAtLeastOneLegendFile) {
      return false;
    }

    if (!validateContents) {
      return true;
    }

    try {
      this.getValidatedContents(repoTree, id);
      return true;
    } catch {
      return false;
    }
  }

  private static async getValidatedContents(repoTree: GitTree, id: string) {
    const isValidShape = await TableDatasetV3.isValidDataset(repoTree, id, false);
    if (!isValidShape) {
      return;
    }

    const titleFilePath = repoTree.join(id, '.table-dataset', 'meta', 'title');
    const title = await titleFilePath.readFile({ encoding: 'utf-8' }).then((text) => text.trim());

    const descriptionFilePath = repoTree.join(id, '.table-dataset', 'meta', 'description');
    let description: string | undefined = undefined;
    if (await descriptionFilePath.isFile) {
      description = await descriptionFilePath.readFile({ encoding: 'utf-8' }).then((text) => text.trim());
    }

    const pathStructurePath = repoTree.join(id, '.table-dataset', 'meta', 'path-structure.json');
    const pathStuctureBuffer = await pathStructurePath.readFile();
    const pathStructure = PathStructure.fromBuffer(pathStuctureBuffer);

    const schemaFilePath = repoTree.join(id, '.table-dataset', 'meta', 'schema.json');
    const schemaBuffer = await schemaFilePath.readFile();
    const schema = Schema.fromBuffer(schemaBuffer);

    const legendDirPath = repoTree.join(id, '.table-dataset', 'meta', 'legend');
    const legendFiles = await legendDirPath.readDirectory();
    const legends = new Legends();
    for (const fileInfo of legendFiles) {
      const legendFile = legendDirPath.join(fileInfo.name);
      const legendFileBuffer = await legendFile.readFile();
      const legend = Legend.fromBuffer(legendFileBuffer);
      legends.add(legend);
    }

    const crss = new CRSs();
    const hasCrsFolder = await repoTree.join(id, '.table-dataset', 'meta', 'crs').isDirectory;
    if (hasCrsFolder) {
      await repoTree
        .join(id, '.table-dataset', 'meta', 'crs')
        .readDirectory()
        .then((paths) => {
          const promises = paths
            .filter((path) => path.isFile)
            .filter((filePath) => filePath.extension === 'wkt')
            .map(async (wktFileInfo) => {
              const wktFilePath = repoTree.join(id, '.table-dataset', 'meta', 'crs', wktFileInfo.name);
              const wktFileBuffer = await wktFilePath.readFile();
              crss.add(CRS.fromWktBuffer(wktFileInfo.name, wktFileBuffer));
            });
          return Promise.all(promises);
        });
    }

    return {
      title,
      description,
      pathStructure,
      schema,
      legends,
      crss,
    };
  }

  private get featureDirectoryTree() {
    return this.tree.join('.table-dataset', 'feature');
  }

  /**
   * Counts the number of files in the feature directory.
   */
  private static async getFeatureCount(featureDirectoryTree: GitTree) {
    const exists = await featureDirectoryTree.exists;
    if (!exists) {
      return 0;
    }

    return (await Array.fromAsync(featureDirectoryTree.walkDirectory(undefined, true))).length;
  }

  /**
   * Yields every raw feature in the dataset.
   *
   * This method will walk through the feature folder tree and yield
   * every raw feature found in a terminal branch.
   */
  async *rawFeatures(concurrentLimit?: number): AsyncGenerator<RawFeature, void, void> {
    for await (const [path, content] of this.featureDirectoryTree.walkDirectory(concurrentLimit)) {
      if (content) {
        yield RawFeature.fromBuffer(path.name, content);
      }
    }
  }

  /**
   * Returns every raw feature in the dataset.
   *
   * For large datasets, consider using the `rawFeatures()`
   * generator method instead to avoid high memory usage.
   */
  async toRawFeatures() {
    const rawFeatures = new RawFeatures();

    for await (const rawFeature of this.rawFeatures()) {
      rawFeatures.add(rawFeature);
    }

    return rawFeatures;
  }

  /**
   * Yields every feature in the dataset.
   *
   * This method will walk through the feature folder tree and yield
   * every feature found in a terminal branch.
   */
  async *features(): AsyncGenerator<Feature, void, void> {
    for await (const rawFeature of this.rawFeatures()) {
      yield rawFeature.toFeature(this.schema, this.legends, this.pathStructure, this.crss);
    }
  }

  /**
   * Returns every feature in the dataset.
   *
   * For large datasets, consider using the `features()`
   * generator method instead to avoid high memory usage.
   * This method is a wrapper around that generator.
   */
  async toFeatures() {
    const features = new Features();

    for (const feature of await this.toRawFeatures()) {
      features.add(feature.toFeature(this.schema, this.legends, this.pathStructure, this.crss));
    }

    return features;
  }

  /**
   * Checks is a feature with the given encoded ID exists in the dataset.
   *
   * A feature's encoded ID is the same as its path in the dataset's feature directory,
   * determined by the dataset's path structure and the feature's primary key values.
   *
   * An existing feature's encoded ID can be retreived with `Feature.eid`.
   */
  async has(eid: string) {
    const featurePath = this.featureDirectoryTree.join(eid);
    return featurePath.isFile;
  }

  /**
   * Gets a feature in the dataset by its encoded ID.
   *
   * A feature's encoded ID is the same as its path in the dataset's feature directory,
   * determined by the dataset's path structure and the feature's primary key values.
   *
   * An existing feature's encoded ID can be retreived with `Feature.eid`.
   */
  async get(eid: string) {
    if (!(await this.has(eid))) {
      return undefined;
    }

    const featurePath = this.featureDirectoryTree.join(eid);
    const blob = await featurePath.readFile();
    const rawFeature = RawFeature.fromBuffer(featurePath.basename, blob);
    return rawFeature.toFeature(this.schema, this.legends, this.pathStructure, this.crss);
  }

  /**
   * Gets multiple features by their encoded IDs.
   *
   * This a convenience method that calls `get` for each encoded ID
   * and returns the found features as a `Features` collection.
   */
  async select(eids: string[]) {
    const features = new Features();
    const pendingPromises: Promise<void>[] = [];

    for (const eid of eids) {
      const promise = this.get(eid).then((feature) => {
        if (feature) {
          features.add(feature);
        }
      });
      pendingPromises.push(promise);
    }

    await Promise.all(pendingPromises);
    return features;
  }

  /**
   * The location of the generated spatial index for this dataset, if it exists.
   */
  private get spatialIndexPath() {
    return this.generatedPath.join('spatial_index.fb');
  }

  /**
   * The location of a reference mapping file that maps spatial index entries to  encoded feature IDs.
   */
  private get spatialIndexRefPath() {
    return this.generatedPath.join('spatial_index.fb.ref');
  }

  /**
   * Gets all features that intersect with the given bounding box.
   *
   * @param bbox - The bounding box to check for intersection, in the format [minX, minY, maxX, maxY].
   */
  async selectIntersection(bbox: [number, number, number, number]) {
    let sIndex: Flatbush;
    let eidIndex: string[];

    // create a spatial index on the dataset if one does not already exist
    if (!this.spatialIndexPath.exists || !this.spatialIndexRefPath.exists) {
      sIndex = new Flatbush(this.featureCount);
      eidIndex = [];

      for await (const feature of this.features()) {
        const bbox = feature.toBbox();
        if (bbox) {
          sIndex.add(bbox[0], bbox[1], bbox[2], bbox[3]);
          eidIndex.push(feature.metadata.eid);
        }
      }

      sIndex.finish();
      this.spatialIndexPath.parentPath!.makeDirectory({ recursive: true });
      this.spatialIndexPath.writeFile(new Uint8Array(sIndex.data.slice()));
      this.spatialIndexRefPath.writeFile(serializer.encode(eidIndex));
    }

    // otherwise, load the existing spatial index
    else {
      const sIndexBuffer = this.spatialIndexPath.readFileSync();
      sIndex = Flatbush.from(
        sIndexBuffer.buffer.slice(sIndexBuffer.byteOffset, sIndexBuffer.byteOffset + sIndexBuffer.byteLength)
      );
      const eidIndexBuffer = this.spatialIndexRefPath.readFileSync();
      eidIndex = serializer.decode(eidIndexBuffer) as string[];
    }

    // query the spatial index for intersecting features
    const intersectingIndices = sIndex.search(bbox[0], bbox[1], bbox[2], bbox[3]);
    const intersectingEids = intersectingIndices
      .map((index) => eidIndex[index])
      .filter((x): x is string => !!x);
    return this.select(intersectingEids);
  }

  /**
   * Yields every feature in the dataset in its GeoJSON feature form.
   *
   * This method will walk through the feature folder tree and yield
   * every feature found in a terminal branch in its GeoJSON form.
   *
   * When `serializable` is true, the yielded GeoJSON features
   * will have a `toJSON` method that uses the Kart serializer
   * to serialize the feature properly. Without this, `JSON.stringify`
   * will fail to serialize certain property types correctly.
   */
  async *geojsonFeatures({ serializable = true } = {}) {
    for await (const feature of this.features()) {
      yield feature.toGeoJSON({ serializable });
    }
  }

  /**
   * Returns the dataset as a GeoJSON FeatureCollection.
   *
   * For large datasets, consider using the `geojsonFeatures()`
   * generator method instead to avoid high memory usage.
   *
   * The result of this method will be cached after the first call.
   *
   * Depending on the size of the dataset, this method may take
   * a while. When at all possible, avoid requesting the entire
   * GeoJSON representation of large datasets.
   */
  async toGeoJSON() {
    this.cache.geoJSON ??= (await this.toFeatures()).toGeoJSON({ serializable: true });
    return this.cache.geoJSON;
  }
}

export interface TableDatasetV3Cache {
  geoJSON?: KartFeatureCollection;
}
