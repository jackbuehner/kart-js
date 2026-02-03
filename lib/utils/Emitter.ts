/**
 * Type mapping for event names to their callback signatures.
 * Each key is an event name, and each value is an object with named parameters.
 */
export type EventMap = Record<string, Record<string, any>>;

/**
 * Generic event emitter class supporting typed event names and signatures.
 *
 * @template T - Event map where keys are event names and values are objects with named parameters
 *
 * @example
 * ```ts
 * interface MyEvents extends EventMap {
 *   'user:login': { userId: string; timestamp: number };
 *   'user:logout': { userId: string };
 * }
 *
 * const emitter = new Emitter<MyEvents>();
 * emitter.on('user:login', (data) => {
 *   console.log(`User ${data.userId} logged in at ${data.timestamp}`);
 * });
 * emitter.emit('user:login', { userId: 'user123', timestamp: Date.now() });
 * ```
 */
export class Emitter<T extends EventMap> {
  private _listeners: Map<keyof T, Set<Function>> = new Map();

  /**
   * Register an event handler that listens to a specified event.
   */
  on<K extends keyof T>(event: K, listener: (data: T[K]) => void): this {
    if (!this._listeners.has(event)) {
      this._listeners.set(event, new Set());
    }
    this._listeners.get(event)!.add(listener);
    return this;
  }

  /**
   * Register a one-time event handler for a specified event.
   * The handler will be automatically removed after the first emission.
   */
  once<K extends keyof T>(event: K, listener: (data: T[K]) => void): this {
    const onceWrapper = (data: T[K]) => {
      listener(data);
      this.off(event, onceWrapper);
    };

    return this.on(event, onceWrapper);
  }

  /**
   * Remove a specific event handler for a specified event.
   */
  off<K extends keyof T>(event: K, listener: (data: T[K]) => void): this;

  /**
   * Remove all event handlers for a specified event.
   */
  off<K extends keyof T>(event: K): this;

  /**
   * Remove all event handlers for all events.
   */
  off(): this;

  off<K extends keyof T>(event?: K, listener?: (data: T[K]) => void): this {
    // Remove all listeners for all events
    if (event === undefined) {
      this._listeners.clear();
      return this;
    }

    // Get the listeners set for this event
    const eventListeners = this._listeners.get(event);
    if (!eventListeners) {
      return this;
    }

    // Remove all listeners for this event
    if (listener === undefined) {
      this._listeners.delete(event);
      return this;
    }

    // Remove specific listener
    eventListeners.delete(listener);
    if (eventListeners.size === 0) {
      this._listeners.delete(event);
    }

    return this;
  }

  /**
   * Emit an event, invoking all handlers registered for it.
   */
  protected emit<K extends keyof T>(event: K, data: T[K]): boolean {
    const eventListeners = this._listeners.get(event);

    if (!eventListeners || eventListeners.size === 0) {
      return false;
    }

    // Create a copy to avoid issues if listeners modify the set
    const listeners = Array.from(eventListeners);
    for (const listener of listeners) {
      (listener as (data: T[K]) => void)(data);
    }

    return true;
  }

  /**
   * Retrieve the event handlers registered for a specific event.
   */
  protected listeners<K extends keyof T>(event: K): Array<(data: T[K]) => void> {
    const eventListeners = this._listeners.get(event);
    return eventListeners ? (Array.from(eventListeners) as Array<(data: T[K]) => void>) : [];
  }

  /**
   * Get the count of listeners for a specific event.
   */
  protected listenerCount<K extends keyof T>(event: K): number;

  /**
   * Get the count of all event handlers in total.
   */
  protected listenerCount(): number;

  protected listenerCount<K extends keyof T>(event?: K): number {
    if (event === undefined) {
      // Count all listeners across all events
      let total = 0;
      for (const listenerSet of this._listeners.values()) {
        total += listenerSet.size;
      }
      return total;
    }

    const eventListeners = this._listeners.get(event);
    return eventListeners ? eventListeners.size : 0;
  }

  /**
   * Check if there are any handlers registered for a specific event.
   */
  protected hasListeners<K extends keyof T>(event: K): boolean;

  /**
   * Check if there are any handlers registered for any event.
   */
  protected hasListeners(): boolean;

  protected hasListeners<K extends keyof T>(event?: K): boolean {
    if (event === undefined) {
      return this._listeners.size > 0;
    }

    const eventListeners = this._listeners.get(event);
    return eventListeners ? eventListeners.size > 0 : false;
  }
}
