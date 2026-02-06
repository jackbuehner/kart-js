import Flatbush from 'flatbush';
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
  readonly path: Path;
  readonly metaPath: Path;

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

  constructor(repoPath: Path, id: string) {
    if (!TableDatasetV3.isValidDataset(repoPath, id)) {
      throw new Error(`Dataset with id "${id}" does not exist or is not a valid table dataset v3.`);
    }

    this.path = repoPath.join(id);
    this.id = id;

    const metaPath = this.path.parentPath?.parentPath?.join('.kartjs', this.id);
    if (!metaPath) {
      throw new Error('Failed to get .kartjs path.');
    }
    metaPath.makeDirectory({ recursive: true });
    this.metaPath = metaPath;

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
      this.crss = validatedContents.crss;
      this.featureCount = this.getFeatureCount();
    } catch (error) {
      const toThrow = new Error(`Dataset with id "${id}" has invalid contents: ${(error as Error).message}`);
      if (error instanceof Error) {
        toThrow.stack = error.stack;
        toThrow.cause = error.cause;
        toThrow.name = error.name;
      }
      throw toThrow;
    }

    // this.working = new WorkingFeatureCollection(this.id, this.toFeatureCollection(), this.schema, this.crss);
    this.working = new WorkingFeatureCollection(this);
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

    const crss = new CRSs();
    const hasCrsFolder = repoDir.join(id, '.table-dataset', 'meta', 'crs').exists;
    if (hasCrsFolder) {
      repoDir
        .join(id, '.table-dataset', 'meta', 'crs')
        .readDirectorySync()
        .filter((path) => path.isFile)
        .filter((filePath) => filePath.extension === 'wkt')
        .forEach((wktFilePath) => {
          crss.add(CRS.fromWktFile(wktFilePath));
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

  private get featureDirectoryPath() {
    return this.path.join('.table-dataset', 'feature');
  }

  /**
   * Counts the number of files in the feature directory.
   */
  private getFeatureCount() {
    if (!this.featureDirectoryPath.exists) {
      return 0;
    }

    const featureFiles = this.featureDirectoryPath
      .readDirectorySync({ recursive: true })
      .filter((file) => file.isFile);
    return featureFiles.length;
  }

  /**
   * Walks the feature directory tree and yields every file node
   * that is in a terminal branch.
   */
  async *terminalBranchNodes(): AsyncGenerator<Path, void, void> {
    if (!this.featureDirectoryPath.exists) {
      return; // a missing folder indicates no features
    }

    const levels = this.pathStructure.levels;
    async function* walk(currentPath: Path, depth = 0): AsyncGenerator<Path, void, void> {
      if (!currentPath.exists) {
        return;
      }

      // TODO: once @zenfs/core supports fs.opendir with options (for recursion), use that instead of readDirectory so the generator truly only loads one file at a time
      const directoryGenerator = currentPath.openDirectory();
      const firstNode = await directoryGenerator.next();

      if (!firstNode.value) {
        // there are no items in this directory
        return;
      }

      // All nodes in the directory should be the same type, so we should
      // yield files if the first node is a file.
      const isTerminalDepth = depth === levels;
      if (firstNode.value.isFile && isTerminalDepth) {
        yield firstNode.value;
        for await (const node of directoryGenerator) {
          if (node.isFile) {
            yield node;
          }
        }
        return;
      }

      // Then, we need to recurse into this directory's subdirectories
      // to yield terminal branch nodes.
      if (firstNode.value.isDirectory && !isTerminalDepth) {
        yield* walk(firstNode.value, depth + 1);
        for await (const node of directoryGenerator) {
          if (node.isDirectory) {
            yield* walk(node, depth + 1);
          }
        }
      }
    }

    yield* walk(this.featureDirectoryPath);
  }

  /**
   * Yields every raw feature in the dataset.
   *
   * This method will walk through the feature folder tree and yield
   * every raw feature found in a terminal branch.
   */
  async *rawFeatures(): AsyncGenerator<RawFeature, void, void> {
    for await (const fileNode of this.terminalBranchNodes()) {
      yield RawFeature.fromFile(fileNode);
    }
  }

  /**
   * Returns every raw feature in the dataset.
   *
   * For large datasets, consider using the `rawFeatures()`
   * generator method instead to avoid high memory usage.
   */
  toRawFeatures() {
    const rawFeatures = new RawFeatures();

    this.featureDirectoryPath
      .readDirectorySync({ recursive: true })
      .filter((file) => file.isFile)
      .forEach((file) => {
        rawFeatures.add(RawFeature.fromFile(file));
      });

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
  toFeatures() {
    const features = new Features();

    for (const feature of this.toRawFeatures()) {
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
  has(eid: string) {
    const featurePath = this.featureDirectoryPath.join(eid);
    return featurePath.exists && featurePath.isFile;
  }

  /**
   * Gets a feature in the dataset by its encoded ID.
   *
   * A feature's encoded ID is the same as its path in the dataset's feature directory,
   * determined by the dataset's path structure and the feature's primary key values.
   *
   * An existing feature's encoded ID can be retreived with `Feature.eid`.
   */
  get(eid: string) {
    if (!this.has(eid)) {
      return undefined;
    }

    const featurePath = this.featureDirectoryPath.join(eid);
    const rawFeature = RawFeature.fromFile(featurePath);
    return rawFeature.toFeature(this.schema, this.legends, this.pathStructure, this.crss);
  }

  /**
   * Gets multiple features by their encoded IDs.
   *
   * This a convenience method that calls `get` for each encoded ID
   * and returns the found features as a `Features` collection.
   */
  select(eids: string[]) {
    const features = new Features();

    for (const eid of eids) {
      const feature = this.get(eid);
      if (feature) {
        features.add(feature);
      }
    }

    return features;
  }

  private get spatialIndexPath() {
    return this.metaPath.join('spatial_index.fb');
  }

  private get spatialIndexRefPath() {
    return this.metaPath.join('spatial_index.fb.ref');
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
    this.cache.geoJSON ??= this.toFeatures().toGeoJSON({ serializable: true });
    return this.cache.geoJSON;
  }
}

export interface TableDatasetV3Cache {
  geoJSON?: KartFeatureCollection;
}
