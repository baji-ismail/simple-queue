type EventType =
  | "itemRemoved"
  | "itemsRemoved"
  | "itemPushed"
  | "itemsPushed"
  | "sizeChanged"
  | "queueCleared"
  | "full"
  | "empty";

type QueueType = "FIFO" | "LIFO";

/**
 * Represents a queue data structure that follows the First In First Out (FIFO) principle.
 * @template T The type of elements stored in the queue.
 */
class Queue<T> {
  private maxsize: number;
  private queueType: QueueType = "FIFO";
  private isClear: boolean = false;
  private items: T[] = [];
  private waiting: (() => void)[] = [];
  private waitingPush: (() => void)[] = [];
  private eventListeners: { [event: string]: ((arg?: T | T[]) => void)[] } = {};

  async *[Symbol.asyncIterator](): AsyncIterableIterator<T> {
    while (true) {
      yield await this.get();
    }
  }

  /**
   * Creates an instance of Queue.
   * @param {Object} [options] The options for configuring the queue.
   * @param {number} [options.maxsize=0] The maximum size of the queue. Use 0 for unlimited size.
   * @param {"FIFO" | "LIFO"} [options.queueType="FIFO"] - The type of queue: "FIFO" for First In First Out or "LIFO" for Last In First Out.
   */
  constructor({
    maxsize = 0,
    queueType = "FIFO",
  }: { maxsize?: number; queueType?: QueueType } = {}) {
    this.maxsize = maxsize;
    this.queueType = queueType;
  }

  /**
   * Removes and returns an item from the queue.
   * If the queue is empty, it will wait until an item is available.
   * If a timeout value is provided and the item is not available within the specified time, it will throw a "Timeout" error.
   * @param {number} [timeout] The maximum time to wait for an item to become available (in milliseconds).
   * @returns {Promise<T>} A promise that resolves with the next item in the queue when available.
   * @throws {Error} If the queue is empty and the timeout is reached.
   */

  async get(timeout?: number): Promise<T> {
    if (this.isEmpty()) {
      try {
        await new Promise<void>((resolve, reject) => {
          this.waiting.push(resolve);
          if (timeout) {
            setTimeout(() => {
              this.waiting = this.waiting.filter(
                (oneResolve) => oneResolve != resolve
              );
              reject("timeout");
            }, timeout);
          }
        });
      } catch {
        throw new Error("Timeout");
      }
    }
    const value = this.items.shift()!;
    if (this.waitingPush.length > 0) {
      const resolve = this.waitingPush.shift()!;
      resolve();
    }
    this.emit("itemRemoved", value);
    if (this.isEmpty()) this.emit("empty");
    return value;
  }

  /**
   * Removes and returns an item from the queue if the queue is not empty.
   * If the queue is empty, it returns undefined.
   * @returns {T | undefined} The next item in the queue, or undefined if the queue is empty.
   */

  get_nowait(): T | undefined {
    const value = this.items.shift();
    if (this.waitingPush.length > 0) {
      const resolve = this.waitingPush.shift()!;
      resolve();
    }
    if (value) this.emit("itemRemoved", value);
    if (value && this.isEmpty()) this.emit("empty");
    return value;
  }

  /**
   * Removes and returns a batch of items from the queue.
   * If the queue does not have enough items to form a full batch, it will wait until enough items are available.
   * @param {number} [count=1] The number of items to retrieve in the batch.
   * @returns {Promise<T[]>} A promise that resolves with an array of items from the queue.
   */

  async getBatch(count: number = 1): Promise<T[]> {
    const values: T[] = [];
    while (values.length < count) {
      const item = await this.get();
      values.push(item);
    }
    this.emit("itemsRemoved", values);
    return values;
  }

  /**
   * Returns the next item in the queue without removing it.
   * If the queue is empty, it returns undefined.
   * @returns {T | undefined} The next item in the queue, or undefined if the queue is empty.
   */

  peek(): T | undefined {
    return this.items[0];
  }

  /**
   * Adds an item to the queue.
   * If the queue is full, it will wait until a slot is available.
   * If a timeout value is provided and the queue remains full for longer than the specified time, it will throw a "Timeout" error.
   * @param {T} item The item to add to the queue.
   * @param {number} [timeout] The maximum time to wait for a slot to become available (in milliseconds).
   * @returns {Promise<void>} A promise that resolves when the item is successfully added to the queue.
   * @throws {Error} If the queue is full and the timeout is reached.
   * @throws {Error} If the queue has been cleared using the `clear` method.
   */

  async push(item: T, timeout?: number): Promise<void> {
    if (this.isFull()) {
      try {
        await new Promise<void>((resolve, reject) => {
          this.waitingPush.push(resolve);

          if (timeout) {
            setTimeout(() => {
              this.waitingPush = this.waitingPush.filter(
                (oneResolve) => oneResolve != resolve
              );
              reject("timeout");
            }, timeout);
          }
        });
      } catch {
        throw new Error("Timeout");
      }
    }
    if (this.isClear) throw new Error("ValuesHasClear");

    if (this.queueType == "FIFO") this.items.push(item);
    else this.items.unshift(item);

    if (this.isFull()) this.emit("full");
    if (this.waiting.length > 0) {
      const resolve = this.waiting.shift()!;
      resolve();
    }
    this.emit("itemPushed", item);
  }

  /**
   * Adds an item to the queue without waiting.
   * If the queue is full, it throws an error.
   * @param {T} item The item to add to the queue.
   * @throws {Error} If the queue is full.
   */

  push_nowait(item: T): void {
    if (this.isFull()) throw new Error("QueueFull");

    if (this.queueType == "FIFO") this.items.push(item);
    else this.items.unshift(item);

    if (this.isFull()) this.emit("full");
    if (this.waiting.length > 0) {
      const resolve = this.waiting.shift()!;
      resolve();
    }
    this.emit("itemPushed", item);
  }

  /**
   * Adds an array of items to the queue.
   * If the queue is full, it will wait until slots are available.
   * @param {T[]} values The array of items to add to the queue.
   * @returns {Promise<void>} A promise that resolves when all items are successfully added to the queue.
   */
  async pushBatch(values: T[]): Promise<void> {
    for (const value of values) {
      await this.push(value);
    }
    this.emit("itemsPushed", values);
  }

  /**
   * Checks if the queue is empty.
   * @returns {boolean} true if the queue is empty, false otherwise.
   */

  isEmpty(): boolean {
    return this.qsize() === 0;
  }

  /**
   * Checks if the queue is full.
   * @returns {boolean} true if the queue is full, false otherwise.
   */
  isFull(): boolean {
    if (!this.maxsize) return false;
    return this.qsize() >= this.maxsize;
  }

  /**
   * Removes all items from the queue and resets the queue to its initial state.
   * Emits a "queueCleared" event after clearing the queue.
   */

  clear() {
    this.isClear = true;
    for (let index = 0; index < this.waitingPush.length; index++) {
      const resolve = this.waitingPush.shift()!;
      resolve();
    }
    this.items = [];
    this.isClear = false;
    this.emit("queueCleared");
  }

  /**
   * Returns the number of items in the queue.
   * @returns {number} The number of items in the queue.
   */

  qsize(): number {
    return this.items.length;
  }

  /**
   * Changes the maximum size of the queue.
   * @param {number} newSize The new maximum size of the queue.
   * @param {boolean} [forceResize=false] If true, the queue will be resized even if newSize is smaller than the current queue size.
   * @param {boolean} [truncateItems=false] If true, items will be removed from the queue if newSize is smaller than the current queue size.
   * @throws {Error} If forceResize is false and newSize is smaller than the current number of items in the queue.
   */
  setSize(
    newSize: number,
    forceResize?: boolean,
    truncateItems?: boolean
  ): void {
    if (!forceResize && newSize < this.qsize()) throw new Error("SizeError");
    if (truncateItems) this.items = this.items.slice(0, newSize);
    this.maxsize = newSize;
    this.emit("sizeChanged");
  }

  /**
   * Registers an event listener for the specified event.
   * When the event is emitted, the listener function will be called with the emitted arguments.
   *
   * @param {string} event The name of the event to listen for.
   * @param {Function} listener The listener function to call when the event is emitted.
   *
   * @event itemRemoved Emitted when an item is removed from the queue.
   * @event itemsRemoved Emitted when multiple items are removed from the queue.
   * @event itemPushed Emitted when an item is pushed to the queue.
   * @event itemsPushed Emitted when multiple items are pushed to the queue.
   * @event sizeChanged Emitted when the maximum size of the queue is changed.
   * @event queueCleared Emitted when all items are removed from the queue, clearing it.
   * @event full Emitted when the queue becomes full.
   * @event empty Emitted when the queue becomes empty.
   */

  on(event: EventType, listener: (arg?: T | T[]) => void): void {
    if (!this.eventListeners[event]) {
      this.eventListeners[event] = [];
    }
    this.eventListeners[event].push(listener);
  }

  /**
   * Emits an event, calling all registered listeners for that event with the provided arguments.
   *
   * @param {string} event The name of the event to emit.
   * @param {...any} args The arguments to pass to the event listeners.
   */
  emit(event: EventType, args?: T | T[]): void {
    if (this.eventListeners[event]) {
      this.eventListeners[event].forEach((listener) => listener(args));
    }
  }
}

export default Queue;
