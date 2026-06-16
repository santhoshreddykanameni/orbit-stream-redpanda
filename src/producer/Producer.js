const { CompressionTypes } = require("kafkajs");

const { serializer } = require("@orbit-stream/core");

class Producer {
  constructor(kafka, config = {}) {
    this.producer = kafka.producer({
      allowAutoTopicCreation: true,

      transactionTimeout: 30000,

      maxInFlightRequests: config.maxInFlightRequests || 10,

      idempotent: config.idempotent ?? true,
    });

    this.connected = false;

    this.compression = config.compression ?? CompressionTypes.None;

    this.acks = config.acks ?? -1;

    this.timeout = config.timeout ?? 30000;

    /*
     * Protect against broker limits.
     *
     * Redpanda:
     * kafka_batch_max_bytes = 16MB
     *
     * Keep SDK batches below that.
     */
    this.maxBatchBytes = config.maxBatchBytes || 8 * 1024 * 1024;
  }

  async connect() {
    if (this.connected) {
      return;
    }

    await this.producer.connect();

    this.connected = true;
  }

  async disconnect() {
    if (!this.connected) {
      return;
    }

    await this.producer.disconnect();

    this.connected = false;
  }

  serializeValue(value) {
    if (value == null) {
      return null;
    }

    if (Buffer.isBuffer(value)) {
      return value;
    }

    return serializer.serialize(value);
  }

  serializeKey(key) {
    if (key == null) {
      return null;
    }

    if (Buffer.isBuffer(key)) {
      return key;
    }

    return String(key);
  }

  serializeHeaders(headers = {}) {
    const result = {};

    for (const [key, value] of Object.entries(headers)) {
      if (value == null) {
        continue;
      }

      result[key] = Buffer.isBuffer(value) ? value : String(value);
    }

    return result;
  }

  estimateMessageSize(message) {
    let size = 0;

    if (message.key) {
      size += Buffer.byteLength(String(message.key));
    }

    if (message.value) {
      if (Buffer.isBuffer(message.value)) {
        size += message.value.length;
      } else {
        size += Buffer.byteLength(JSON.stringify(message.value));
      }
    }

    if (message.headers) {
      for (const [key, value] of Object.entries(message.headers)) {
        size += Buffer.byteLength(key);

        if (Buffer.isBuffer(value)) {
          size += value.length;
        } else {
          size += Buffer.byteLength(String(value));
        }
      }
    }

    return size;
  }

  async sendBatch(topic, messages) {
    const kafkaMessages = new Array(messages.length);

    for (let i = 0; i < messages.length; i++) {
      const msg = messages[i];

      kafkaMessages[i] = {
        key: this.serializeKey(msg.key),

        value: this.serializeValue(msg.value),

        headers: this.serializeHeaders(msg.headers),
      };
    }

    await this.producer.send({
      topic,

      messages: kafkaMessages,

      compression: this.compression,

      acks: this.acks,

      timeout: this.timeout,
    });
  }

  async publishBatch(topic, messages) {
    if (!messages?.length) {
      return;
    }

    let batch = [];

    let batchBytes = 0;

    for (const message of messages) {
      const messageSize = this.estimateMessageSize(message);

      /*
       * If adding this message exceeds
       * maxBatchBytes, send current batch.
       */
      if (batch.length > 0 && batchBytes + messageSize > this.maxBatchBytes) {
        await this.sendBatch(topic, batch);

        batch = [];

        batchBytes = 0;
      }

      batch.push(message);

      batchBytes += messageSize;
    }

    if (batch.length > 0) {
      await this.sendBatch(topic, batch);
    }
  }
}

module.exports = Producer;
