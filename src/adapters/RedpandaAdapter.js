const { BaseAdapter, BatchProcessor } = require("@orbit-stream/core");

const createKafka = require("../utils/createKafka");

const Producer = require("../producer/Producer");

const Consumer = require("../consumer/Consumer");

class RedpandaAdapter extends BaseAdapter {
  constructor(config = {}) {
    super(config);

    this.kafka = createKafka(config);

    this.producer = new Producer(this.kafka, config);

    this.consumer = new Consumer(this.kafka, config);

    this.batchProcessor = new BatchProcessor({
      batchSize: config.batchSize || 5000,

      flushInterval: config.flushInterval || 100,
    });
  }

  async connect() {
    await Promise.all([this.producer.connect(), this.consumer.connect()]);

    this.emitConnected();
  }

  async disconnect() {
    await Promise.all([this.producer.disconnect(), this.consumer.disconnect()]);

    this.emitDisconnected();
  }

  async publish(topic, message) {
    this.batchProcessor.add(
      message,

      async (batch) => {
        await this.publishBatch(topic, batch);
      },
    );
  }

  async publishBatch(topic, messages) {
    return this.producer.publishBatch(
      topic,

      messages,
    );
  }

  async subscribe(topic, handler, options = {}) {
    return this.consumer.subscribe(
      topic,

      handler,

      options,
    );
  }
}

module.exports = RedpandaAdapter;
