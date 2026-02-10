import { Temporal } from 'temporal-polyfill';
import { Array as YArray, Doc as YDoc, Map as YMap } from 'yjs';
import type { KartFeatureCollection } from '../utils/features/isKartEnabled.ts';

type KartFeature = KartFeatureCollection['features'][number];

type TrackedDelete = { type: 'delete' };
type TrackedInsert = { type: 'insert'; feature: KartFeature };
type TrackedPropertiesUpdate = { type: 'update'; properties: Partial<KartFeature['properties']> };
type TrackedGeometryUpdate = { type: 'update'; geometry: KartFeature['geometry'] };
type TrackedGeometryAndPropertiesUpdate = {
  type: 'update';
  properties: Partial<KartFeature['properties']>;
  geometry: KartFeature['geometry'];
};
export type TrackedChange =
  | TrackedDelete
  | TrackedInsert
  | TrackedPropertiesUpdate
  | TrackedGeometryUpdate
  | TrackedGeometryAndPropertiesUpdate;

type TrackedChangeKeyValue = {
  key: string;
  value: TrackedChange;
};

/**
 * A helper for tracking changes to a table dataset using Yjs.
 *
 * Changes are stored as key-value pairs, where the key is the
 * encoded ID (encoded primary key values) of the feature, and the value is
 * a TrackedChange object describing the type of change and the relevant data.
 *
 * This is a more restricted version of [YKeyValue](https://github.com/yjs/y-utility/blob/main/y-keyvalue.js).
 * See https://discuss.yjs.dev/t/clear-document-history-and-reject-old-updates/945/2#:~:text=Q%3A%20Why%20cannot,development%20of%20this.
 * for why using a YMap is less efficient. It also explains why we track all datasets in a top-level YMap.
 */
export class TrackedChanges implements Omit<
  Map<string, TrackedChange>,
  'set' | 'delete' | 'forEach' | 'entries' | 'values'
> {
  private trackedChangesLocalCache: Map<string, TrackedChange>;
  yarray: YArray<TrackedChangeKeyValue>;
  primaryKeyNames: string[];

  private static getPrefix(datasetId: string) {
    return `${datasetId}‾‾tracked-changes‾‾`;
  }

  static findSharedTypes(
    ydoc: YDoc,
    datasetId: string
  ): {
    globalMap: YMap<unknown>;
    matches: {
      key: string;
      value: unknown;
      timestamp: Temporal.PlainDateTime;
    }[];
  } {
    const yarrayPrefix = this.getPrefix(datasetId);
    const ymap = ydoc.getMap('kartjs');

    // Look for the newest Y.Array in the Y.Doc with the name `${datasetId}‾‾tracked-changes‾‾<ISO 8601>`,
    // where <ISO 8601> is a timestamp of when the Y.Array was created.
    // We always include the timestamp of array creation in case a dataset with the same ID is
    // deleted and re-created, at a later date. If we re-used the same YArray name, a client
    // with a copy of the old Y.Doc that has the old Y.Array could end up overwriting or merging
    // tracked changes from the old datatset into a new dataset that happens to have the same ID,
    // which would be bad.
    const ymapEntries: [string, unknown][] = [];
    for (const entry of ymap) {
      ymapEntries.push(entry);
    }
    const existingArrays = ymapEntries
      .filter(([key]) => {
        return key.startsWith(yarrayPrefix);
      })
      .map(([key, value]) => {
        const timestampStr = key.slice(yarrayPrefix.length);
        const timestamp = Temporal.PlainDateTime.from(timestampStr);
        if (!timestamp) {
          console.warn(`TrackedChanges: Invalid timestamp in Y.Array name: ${key}`);
          return null;
        }
        return {
          key,
          value,
          timestamp,
        };
      })
      .filter((x) => !!x)
      // sort in descending order so the newest Y.Array is first
      .sort((a, b) => Temporal.PlainDateTime.compare(b.timestamp, a.timestamp));

    return {
      globalMap: ymap,
      matches: existingArrays,
    };
  }

  /**
   * Creates a new TrackedChanges instance.

   * @param primaryKeyNames
   * The primary key names of the table dataset.
   * 
   * These are used to ensure that `setProperties` prohibits changing primary key values.
   * If primary keys need to be changed, a delete + insert change should be used instead.
   * 
   * @param ydoc
   * A Y.js document where the tracked changes will be stored. This allows the tracked
   * changes to be synced in real-time between multiple clients or Kart instances.
   * 
   * @param datasetId
   * The name of the dataset for which changes are being tracked.
   * 
   * his is used to find or create the Y.Array in the Y.Doc where changes are stored.
   * The Y.Arrays must be named in the format `${datasetId}‾‾tracked-changes‾‾<ISO 8601 timestamp>` to ensure that:
   * 1) multiple datasets can be tracked in the same Y.Doc without conflicts
   * 2) if a dataset is deleted and re-created with the same ID, old tracked changes from the previous dataset won't be merged into the new dataset's tracked changes.
   */
  constructor(primaryKeyNames: string[], ydoc: YDoc, datasetId: string) {
    this.primaryKeyNames = primaryKeyNames;
    this.trackedChangesLocalCache = new Map<string, TrackedChange>();

    const { globalMap: ymap, matches: existingArrays } = TrackedChanges.findSharedTypes(ydoc, datasetId);
    const newestArray = existingArrays[0];

    // validate that the newest Y.Array has the expected format
    if (newestArray) {
      if (!newestArray.value || !(newestArray.value instanceof YArray)) {
        throw new Error(
          `TrackedChanges: Expected Y.Array for tracked changes, but found ${typeof newestArray.value}`
        );
      }

      if (!isTrackedChangeKeyValueArray(newestArray.value.toArray())) {
        throw new Error(`TrackedChanges: Existing Y.Array for tracked changes has invalid format`);
      }

      this.yarray = newestArray.value as YArray<TrackedChangeKeyValue>;

      // delete older (stale) matches
      for (const array of existingArrays) {
        if (array === newestArray) {
          continue;
        }
        ymap.delete(array.key);
      }

      // clean up duplicate keys (keep furtherst to the right, which are the latest changes)
      ydoc.transact(() => {
        const seenKeys = new Set<string>();
        for (let i = this.yarray.length - 1; i >= 0; i--) {
          const item = this.yarray.get(i);
          if (seenKeys.has(item.key)) {
            this.yarray.delete(i, 1);
          } else {
            seenKeys.add(item.key);
          }
        }
      });

      // populate local cache
      for (const item of this.yarray.toArray()) {
        this.trackedChangesLocalCache.set(item.key, item.value);
      }
    }

    // if there was no existing Y.Array, create a new one with the current timestamp
    else {
      const timestamp = Temporal.Now.plainDateTimeISO().toString();
      const yarrayName = `${TrackedChanges.getPrefix(datasetId)}${timestamp}`;
      const yarray = new YArray<TrackedChangeKeyValue>();
      yarray.doc = ydoc;
      ymap.set(yarrayName, yarray);
      this.yarray = yarray;
    }

    this.yarray.observe((event) => {
      // untrack changes that were removed from the Y.Array
      event.changes.deleted.forEach((item) => {
        const itemContent = item.content.getContent();

        if (!isTrackedChangeKeyValueArray(itemContent)) {
          console.warn('TrackedChanges: Unexpected item content format', itemContent);
          return;
        }

        itemContent.forEach((change) => {
          if (this.trackedChangesLocalCache.has(change.key)) {
            this.trackedChangesLocalCache.delete(change.key);
          }
        });
      });

      // track changes that were added to the Y.Array
      event.changes.added.forEach((item) => {
        const itemContent = item.content.getContent();

        if (!isTrackedChangeKeyValueArray(itemContent)) {
          console.warn('TrackedChanges: Unexpected item content format', itemContent);
          return;
        }

        // walk right-to-left so only the latest value for each key is kept
        for (let i = itemContent.length - 1; i >= 0; i--) {
          const change = itemContent[i];
          if (!change) {
            continue;
          }

          // if this change key is already collected, that means
          // it is a old value that should be ignored
          if (this.trackedChangesLocalCache.has(change.key)) {
            continue;
          }

          // add to map
          this.trackedChangesLocalCache.set(change.key, change.value);
        }
      });
    });
  }

  /**
   * Checks if a TrackedChange exists for the given key.
   */
  has(key: string) {
    return this.trackedChangesLocalCache.has(key);
  }

  /**
   * Gets the TrackedChange for the given key.
   */
  get(key: string) {
    return this.trackedChangesLocalCache.get(key);
  }

  /**
   * Sets the value for the given key in the Y.Array.
   *
   * If the key already exists, the old entry is removed first.
   */
  private set(key: string, value: TrackedChange) {
    if (!this.yarray.doc) {
      throw new Error('Y.Array must be attached to a Y.Doc');
    }

    this.yarray.doc.transact(() => {
      if (this.trackedChangesLocalCache.has(key)) {
        // remove old entry from Y.Array
        this.delete(key);
      }

      this.yarray.push([{ key, value }]);
    });
  }

  /**
   * Deletes the entire contents of the TrackedChanges key-value array.
   */
  clear() {
    if (!this.yarray.doc) {
      throw new Error('Y.Array must be attached to a Y.Doc');
    }

    this.yarray.doc.transact(() => {
      this.yarray.delete(0, this.yarray.length);
    });
  }

  get size() {
    return this.trackedChangesLocalCache.size;
  }

  keys() {
    return this.trackedChangesLocalCache.keys();
  }

  [Symbol.iterator]() {
    return this.trackedChangesLocalCache[Symbol.iterator]();
  }

  get [Symbol.toStringTag]() {
    return 'TrackedChanges';
  }

  /**
   * Deletes the first occurrence of the given key from the Y.Array.
   *
   * To track a deletion, use `setDelete` instead.
   */
  private delete(key: string) {
    if (!this.yarray.doc) {
      throw new Error('Y.Array must be attached to a Y.Doc');
    }

    let index = 0;
    for (const val of this.yarray) {
      if (val.key === key) {
        this.yarray.delete(index, 1);
        break;
      }
      index++;
    }
  }

  /**
   * Track a deletion of a feature.
   */
  setDelete(key: string): this {
    if (!this.has(key)) {
      this.set(key, { type: 'delete' });
      return this;
    }

    const current = this.get(key);
    if (current?.type === 'delete') {
      return this;
    }

    if (current?.type === 'insert') {
      // deleting a feature that already has an insert tracked
      // idicates that we can just remove the tracked change
      this.delete(key);
      return this;
    }

    this.set(key, { type: 'delete' });
    return this;
  }

  /**
   * Track the insertion of a new feature.
   *
   * IMPORTANT: Make sure that the feature ID (key) (eid) matches the primary keys included in the feature properties.
   */
  setInsert(key: string, value: Omit<TrackedInsert, 'type'>): this {
    this.set(key, { type: 'insert', ...value });
    return this;
  }

  /**
   * Register an update to the geometry of a feature.
   */
  setGeometry(key: string, value: Omit<TrackedGeometryUpdate, 'type'>): this {
    if (!this.has(key)) {
      this.set(key, { type: 'update', ...value });
      return this;
    }

    const current = this.get(key)!;

    // merge with existing update
    if (current.type === 'update' && 'properties' in current) {
      this.set(key, {
        type: 'update',
        properties: current.properties,
        geometry: value.geometry,
      });
      return this;
    }

    this.set(key, { type: 'update', ...value });
    return this;
  }

  /**
   * Register an update to the properties of a feature.
   *
   * The properties MUST be all properties that are changed from
   * the original feature, not since the last update.
   *
   * DO NOT pass the full set of properties.
   *
   * DO NOT update primary keys using this method. To update primary keys,
   * delete the feature, calculate the new feature ID based on the new primary keys,
   * and then insert the new feature.
   */
  setProperties(key: string, value: Omit<TrackedPropertiesUpdate, 'type'>): this {
    // if primary keys are being changed, the consumer needs to delete and insert instead
    value.properties ??= {};
    for (const primaryKey of this.primaryKeyNames) {
      if (primaryKey in value.properties) {
        throw new Error(
          `Cannot update primary key "${primaryKey}" using setProperties. To change primary keys, delete the feature and insert a new one instead.`
        );
      }
    }

    if (!this.has(key)) {
      this.set(key, { type: 'update', ...value });
      return this;
    }

    const current = this.get(key)!;

    // merge with existing update geometry
    if (current.type === 'update' && 'geometry' in current) {
      this.set(key, {
        type: 'update',
        properties: value.properties,
        geometry: current.geometry,
      });
      return this;
    }

    this.set(key, { type: 'update', ...value });
    return this;
  }

  /**
   * Similar to `Array.prototype.map`, this method allows mapping over all tracked changes.
   * Unliked `Array.prototype.map`, the callback is called with (value, key, map), not (value, index, array).
   */
  map<U>(callback: (value: TrackedChange, key: string, map: Map<string, TrackedChange>) => U): U[] {
    const results: U[] = [];
    for (const [key, value] of this.trackedChangesLocalCache) {
      results.push(callback(value, key, this.trackedChangesLocalCache));
    }
    return results;
  }
}

/**
 * Whether the given value is an array of TrackedChangeKeyValue objects.
 *
 * The validity of the `fetaure`, `properties`, and `geometry` fields is not checked.
 */
function isTrackedChangeKeyValueArray(toCheck: unknown): toCheck is TrackedChangeKeyValue[] {
  if (!Array.isArray(toCheck)) {
    return false;
  }

  return toCheck.every((item: unknown) => {
    return (
      typeof item === 'object' &&
      item !== null &&
      'key' in item &&
      typeof item.key === 'string' &&
      'value' in item &&
      typeof item.value === 'object' &&
      item.value !== null &&
      'type' in item.value &&
      typeof item.value.type === 'string' &&
      ['delete', 'insert', 'update'].includes(item.value.type)
    );
  });
}
