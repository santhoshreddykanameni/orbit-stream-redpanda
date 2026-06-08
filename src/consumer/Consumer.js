const { serializer } = require("@orbit-stream/core");

class Consumer {
  constructor(kafka, config = {}) {
    this.consumer = kafka.consumer({
      groupId: config.groupId || "orbit-stream-group",

      sessionTimeout: 60000,

      heartbeatInterval: 3000,

      rebalanceTimeout: 120000,

      maxBytesPerPartition: 25 * 1024 * 1024,

      maxBytes: 100 * 1024 * 1024,

      minBytes: 1 * 1024 * 1024,

      maxWaitTimeInMs: 100,
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

    await this.consumer.disconnect();

    this.connected = false;
  }

  async subscribe(topic, handler, options = {}) {
    await this.consumer.subscribe({
      topic,

      fromBeginning: options.fromBeginning || false,
    });

    await this.consumer.run({
      autoCommit: true,

      partitionsConsumedConcurrently:
        options.partitionsConsumedConcurrently || 8,

      eachBatchAutoResolve: true,

      eachBatch: async ({
        batch,
        resolveOffset,
        heartbeat,
        commitOffsetsIfNecessary,
      }) => {
        const messages = new Array(batch.messages.length);

        for (let i = 0; i < batch.messages.length; i++) {
          const msg = batch.messages[i];

          messages[i] = {
            key: msg.key,

            value: serializer.deserialize(msg.value),

            headers: msg.headers,

            timestamp: Number(msg.timestamp),
          };

          resolveOffset(msg.offset);
        }

        await handler(messages);

        await heartbeat();

        await commitOffsetsIfNecessary();
      },
    });
  }
}

module.exports = Consumer;
