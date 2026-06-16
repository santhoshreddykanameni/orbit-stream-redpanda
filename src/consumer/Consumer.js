const { serializer } = require("@orbit-stream/core");

class Consumer {
  constructor(kafka, config = {}) {
    this.consumer = kafka.consumer({
      groupId: config.groupId || "orbit-stream-group",

      sessionTimeout: config.sessionTimeout || 60000,

      heartbeatInterval: config.heartbeatInterval || 3000,

      rebalanceTimeout: config.rebalanceTimeout || 120000,

      maxBytesPerPartition: config.maxBytesPerPartition || 25 * 1024 * 1024,

      maxBytes: config.maxBytes || 100 * 1024 * 1024,

      minBytes: config.minBytes || 1 * 1024 * 1024,

      maxWaitTimeInMs: config.maxWaitTimeInMs || 100,
    });

    this.connected = false;
  }

  async connect() {
    if (this.connected) {
      return;
    }

    await this.consumer.connect();

    this.connected = true;
  }

  async disconnect() {
    if (!this.connected) {
      return;
    }

    try {
      await this.consumer.stop();
    } catch (error) {}

    await this.consumer.disconnect();

    this.connected = false;
  }

  deserializeValue(value) {
    if (!value) {
      return null;
    }

    try {
      return serializer.deserialize(value);
    } catch {
      // Raw telemetry frame / binary payload
      return value;
    }
  }

  normalizeHeaders(headers = {}) {
    const normalized = {};

    for (const [key, value] of Object.entries(headers)) {
      normalized[key] = Buffer.isBuffer(value) ? value.toString() : value;
    }

    return normalized;
  }

  async subscribe(topic, handler, options = {}) {
    console.log("[Consumer] Subscribing to:", topic);

    await this.consumer.subscribe({
      topic,
      fromBeginning: options.fromBeginning || false,
    });

    console.log("[Consumer] Subscribe successful");

    this.consumer.on(this.consumer.events.GROUP_JOIN, (e) => {
      console.log("[Consumer] GROUP_JOIN", e.payload);
    });

    this.consumer.on(this.consumer.events.CRASH, (e) => {
      console.error("[Consumer] CRASH", e.payload.error);
    });

    this.consumer.on(this.consumer.events.CONNECT, () => {
      console.log("[Consumer] CONNECT");
    });

    console.log("[Consumer] Starting run()");

    this.runPromise = this.consumer.run({
      autoCommit: options.autoCommit ?? true,

      partitionsConsumedConcurrently:
        options.partitionsConsumedConcurrently || 8,

      eachBatchAutoResolve: true,

      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
      }) => {
        console.log(
          "[Consumer] Batch received",
          batch.partition,
          batch.messages.length,
        );

        const messages = new Array(batch.messages.length);

        for (let i = 0; i < batch.messages.length; i++) {
          const msg = batch.messages[i];

          messages[i] = {
            key: msg.key ? msg.key.toString() : null,
            value: this.deserializeValue(msg.value),
            headers: this.normalizeHeaders(msg.headers),
            timestamp: Number(msg.timestamp),
            partition: batch.partition,
            offset: msg.offset,
          };

          resolveOffset(msg.offset);
        }

        await handler(messages);

        await heartbeat();

        await commitOffsetsIfNecessary();
      },
    });

    this.runPromise.catch((error) => {
      console.error("[Consumer] Run failed:", error);
    });

    return true;

    console.log("[Consumer] run() returned");
  }
}

module.exports = Consumer;
