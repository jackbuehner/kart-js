// adapted from the example at https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object/freeze#deep_freezing

/**
 * Deeply freezes an object and all of its nested properties IN-PLACE.
 *
 * If you do not want to deeply freeze, just use `Object.freeze()`.
 *
 * @param object - The object to deeply freeze.
 * @param frozen - A WeakSet to track already frozen objects (used internally).
 * @returns The deeply frozen object.
 */
export function deepFreeze<T>(object: T, frozen = new WeakSet<any>()): T {
  // skip if not a a freezable object
  if (
    object === null ||
    (typeof object !== 'object' && typeof object !== 'function') ||
    ArrayBuffer.isView(object)
  ) {
    return object;
  }

  // skip if already frozen
  if (frozen.has(object) || Object.isFrozen(object)) {
    return object;
  }

  frozen.add(object); // track before recursion to handle circular references

  // retrieve the property names defined on object
  const propNames = Reflect.ownKeys(object);

  // freeze properties before freezing self
  for (const name of propNames) {
    const value = Reflect.get(object, name);

    if ((value && typeof value === 'object') || typeof value === 'function') {
      deepFreeze(value, frozen);
    }
  }

  // freeze everything in the object
  for (const key of Reflect.ownKeys(object)) {
    // skip if property is not configurable (e.g., Array length)
    const propertyDescriptor = Object.getOwnPropertyDescriptor(object, key);
    if (!propertyDescriptor || !propertyDescriptor.configurable) {
      continue;
    }

    // skip freezing toJSON to allow it to be redefined by makeSerializable
    if (key === 'toJSON' && Object.isExtensible(object)) {
      Object.defineProperty(object, key, {
        writable: true,
        configurable: true,
      });
      continue;
    }

    Object.defineProperty(object, key, {
      writable: false,
      configurable: true,
    });
  }
  Object.preventExtensions(object);
  return object;
}
