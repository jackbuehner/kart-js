export abstract class Enumerable<T> extends Set<T> implements Iterable<T> {
  constructor(items?: Iterable<T> | T[] | Set<T>) {
    super(new Set(items));
  }

  map<U>(fn: (item: T) => U): U[] {
    const res: U[] = [];
    for (const v of this) res.push(fn(v));
    return res;
  }

  filter(fn: (item: T) => boolean): T[] {
    const res: T[] = [];
    for (const v of this) if (fn(v)) res.push(v);
    return res;
  }

  find(fn: (item: T) => boolean): T | undefined {
    for (const v of this) {
      if (fn(v)) return v;
    }
  }

  toArray(): T[] {
    return [...this];
  }
}
