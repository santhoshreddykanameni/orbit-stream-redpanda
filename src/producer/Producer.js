const { CompressionTypes } = require("kafkajs");

const { serializer } = require("@orbit-stream/core");

class Producer {
  constructor(kafka, config = {}) {
    this.producer = kafka.producer({
      allowAutoTopicCreation: true,

      transactionTimeout: 30000,

      maxInFlightRequests: config.maxInFlightRequests || 10,

      idempotent: false,
    });

    this.connected = false;

    this.compression = config.compression ?? CompressionTypes.None;
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

  async publishBatch(topic, messages) {
    const kafkaMessages = new Array(messages.length);

    for (let i = 0; i < messages.length; i++) {
      const msg = messages[i];

      kafkaMessages[i] = {
        key: msg.key,

        value: serializer.serialize(msg.value),

        headers: msg.headers,
      };
    }

    await this.producer.send({
      topic,

      compression: this.compression,

      acks: 1,

      timeout: 30000,

      messages: kafkaMessages,
    });
  }
}

module.exports = Producer;
