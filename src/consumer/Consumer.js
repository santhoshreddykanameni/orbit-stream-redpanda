const { serializer, logger } = require("@orbit-stream/core");

class Consumer {
  constructor(kafka, config = {}) {
    this.consumer = kafka.consumer({
      groupId: config.groupId || "orbit-stream-group",

      sessionTimeout: 30000,

      heartbeatInterval: 3000,

      maxBytesPerPartition: 10485760,

      minBytes: 1,

      maxBytes: 52428800,
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
        options.partitionsConsumedConcurrently || 3,

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
