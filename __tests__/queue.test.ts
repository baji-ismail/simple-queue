import Queue from "../src/queue.ts";

describe("Queue", () => {
  let queue: Queue<number>;

  beforeEach(() => {
    queue = new Queue<number>();
  });

  it("should check if the queue is full", () => {
    expect(queue.isFull()).toBe(false);
    queue.setSize(2);
    queue.push(1);
    queue.push(2);
    expect(queue.isFull()).toBe(true);
  });

  it("should clear the queue", () => {
    queue.push(1);
    queue.push(2);
    queue.clear();
    expect(queue.isEmpty()).toBe(true);
  });

  it("should throw an error when newSize is smaller than the current number of items in the queue", () => {
    queue.push(1);
    queue.push(2);
    expect(() => queue.setSize(1)).toThrow("SizeError");
  });

  it("should change the maximum size of the queue", () => {
    queue.push(1);
    queue.push(2);
    queue.setSize(1, true, true);
    expect(queue.qsize()).toBe(1);
  });
});

describe("Queue FIFO", () => {
  let queue: Queue<number>;

  beforeEach(() => {
    queue = new Queue<number>();
  });

  it("should add and remove items correctly", async () => {
    queue.push(1);
    queue.push(2);
    expect(await queue.get()).toBe(1);
    expect(await queue.get()).toBe(2);
  });

  it("should add and remove items without waiting", () => {
    queue.push_nowait(1);
    queue.push_nowait(2);
    expect(queue.get_nowait()).toBe(1);
    expect(queue.get_nowait()).toBe(2);
  });

  it("should waiting remove items will received if add happen", async () => {
    const waiting = queue.get();
    queue.push_nowait(1); // no wating push will resolve the waiting get.
    expect(await waiting).toBe(1);
  }, 10); // 10ms is enough to this case.

  it("should waiting remove items will received if add happen", async () => {
    queue.setSize(2);
    await queue.push(1);
    await queue.push(2);
    expect(queue.isFull()).toBe(true);
    // this will be blocked while get has happpen because it's full.
    const waiting = queue.push(3);
    expect(queue.get_nowait()).toBe(1);
    await waiting;
    expect.anything();
  }, 10); // 10ms is enough to this case.

  it("should add and remove items in batch correctly", async () => {
    queue.push(1);
    queue.push(2);
    const batch = await queue.getBatch(2);
    expect(batch).toEqual([1, 2]);
  });

  it("should peek at the next item without removing it", async () => {
    queue.push(1);
    expect(queue.peek()).toBe(1);
    expect(await queue.get()).toBe(1);
  });

  it("should check if the queue is empty", () => {
    expect(queue.isEmpty()).toBe(true);
    queue.push(1);
    expect(queue.isEmpty()).toBe(false);
  });
});

describe("Queue LIFO", () => {
  let queue: Queue<number>;

  beforeEach(() => {
    queue = new Queue<number>({ queueType: "LIFO" });
  });

  it("should add and remove items correctly", async () => {
    queue.push(1);
    queue.push(2);
    expect(await queue.get()).toBe(2);
    expect(await queue.get()).toBe(1);
  });

  it("should add and remove items without waiting", () => {
    queue.push_nowait(1);
    queue.push_nowait(2);
    expect(queue.get_nowait()).toBe(2);
    expect(queue.get_nowait()).toBe(1);
  });

  it("should add and remove items in batch correctly", async () => {
    queue.push(1);
    queue.push(2);
    const batch = await queue.getBatch(2);
    expect(batch).toEqual([2, 1]);
  });

  it("should peek at the next item without removing it", async () => {
    queue.push(1);
    expect(queue.peek()).toBe(1);
    expect(await queue.get()).toBe(1);
  });

  it("should check if the queue is empty", () => {
    expect(queue.isEmpty()).toBe(true);
    queue.push(1);
    expect(queue.isEmpty()).toBe(false);
  });
});

describe("Queue events", () => {
  let queue: Queue<number>;

  beforeEach(() => {
    queue = new Queue<number>();
  });

  it("should register and emit itemRemoved event", () => {
    const listener = jest.fn();
    queue.push(1);
    queue.on("itemRemoved", listener);
    queue.get();
    expect(listener).toHaveBeenCalledWith(1);
  });

  it("should register and emit itemsRemoved event", async () => {
    const listener = jest.fn();
    queue.push(1);
    queue.push(2);
    queue.on("itemsRemoved", listener);
    await queue.getBatch(2);
    expect(listener).toHaveBeenCalledWith([1, 2]);
  });

  it("should register and emit itemPushed event", () => {
    const listener = jest.fn();
    queue.on("itemPushed", listener);
    queue.push(1);
    expect(listener).toHaveBeenCalledWith(1);
  });

  it("should register and emit itemsPushed event", async () => {
    const listener = jest.fn();
    queue.on("itemsPushed", listener);
    await queue.pushBatch([1, 2]);
    expect(listener).toHaveBeenCalledWith([1, 2]);
  });

  it("should register and emit sizeChanged event", () => {
    const listener = jest.fn();
    queue.on("sizeChanged", listener);
    queue.setSize(1);
    expect(listener).toHaveBeenCalled();
  });

  it("should register and emit queueCleared event", () => {
    const listener = jest.fn();
    queue.on("queueCleared", listener);
    queue.push(1);
    queue.clear();
    expect(listener).toHaveBeenCalled();
  });

  it("should register and emit full event", async () => {
    const listener = jest.fn();
    queue.setSize(1);
    queue.on("full", listener);
    await queue.push(1);
    expect(listener).toHaveBeenCalled();
  });

  it("should register and emit empty event", async () => {
    const listener = jest.fn();
    queue.on("empty", listener);
    await queue.push(1);
    await queue.get();
    expect(listener).toHaveBeenCalled();
  });
});
