const { BaseAdapter, BatchProcessor } = require("@orbit-stream/core");

const { ConfigResourceTypes } = require("kafkajs");

const createKafka = require("../utils/createKafka");
const Producer = require("../producer/Producer");
const Consumer = require("../consumer/Consumer");

class RedpandaAdapter extends BaseAdapter {
  constructor(config = {}) {
    super(config);

    this.config = config;

    this.kafka = createKafka(config);

    this.manageTopics = config.manageTopics === true;

    this.admin = this.manageTopics ? this.kafka.admin() : null;

    this.producer = new Producer(this.kafka, config);

    this.consumer = new Consumer(this.kafka, config);

    this.batchProcessor = new BatchProcessor({
      batchSize: config.batchSize || 5000,

      maxBatchBytes: config.maxBatchBytes || 4 * 1024 * 1024,

      flushInterval: config.flushInterval || 100,
    });

    this.publishCallback = async (topic, batch) => {
      await this.publishBatch(topic, batch);
    };
  }

  async ensureTopic(topicConfig, existingTopics) {
    if (!this.admin) {
      return;
    }

    const {
      name: topic,
      partitions = 1,
      replicationFactor = 1,
      retention,
    } = topicConfig;

    const exists = existingTopics.has(topic);

    const configEntries = [];

    if (retention?.days) {
      configEntries.push({
        name: "retention.ms",

        value: String(retention.days * 24 * 60 * 60 * 1000),
      });
    }

    if (retention?.maxSizeGB) {
      configEntries.push({
        name: "retention.bytes",

        value: String(retention.maxSizeGB * 1024 * 1024 * 1024),
      });
    }

    if (!exists) {
      await this.admin.createTopics({
        waitForLeaders: true,

        topics: [
          {
            topic,

            numPartitions: partitions,

            replicationFactor,

            configEntries,
          },
        ],
      });

      console.log(`[OrbitStream] Created topic '${topic}'`);

      existingTopics.add(topic);

      return;
    }

    if (configEntries.length === 0) {
      return;
    }

    try {
      await this.admin.alterConfigs({
        resources: [
          {
            type: ConfigResourceTypes.TOPIC,

            name: topic,

            configEntries,
          },
        ],
      });

      console.log(`[OrbitStream] Updated topic '${topic}'`);
    } catch (error) {
      console.warn(
        `[OrbitStream] Failed to update topic '${topic}'`,
        error.message,
      );
    }
  }

  async setupTopics() {
    if (!this.admin || !Array.isArray(this.config.topics)) {
      return;
    }

    const existingTopics = new Set(await this.admin.listTopics());

    for (const topic of this.config.topics) {
      await this.ensureTopic(topic, existingTopics);
    }
  }

  async connect() {
    if (this.admin) {
      await this.admin.connect();
    }

    try {
      await this.setupTopics();

      await Promise.all([this.producer.connect(), this.consumer.connect()]);

      this.emitConnected();
    } catch (error) {
      if (this.admin) {
        await this.admin.disconnect().catch(() => {});
      }

      throw error;
    }
  }

  async disconnect() {
    await Promise.allSettled([
      this.producer.disconnect(),
      this.consumer.disconnect(),
    ]);

    if (this.admin) {
      await this.admin.disconnect().catch(() => {});
    }

    this.emitDisconnected();
  }

  async publish(topic, message) {
    this.batchProcessor.add(message, (batch) =>
      this.publishCallback(topic, batch),
    );
  }

  async publishBatch(topic, messages) {
    if (!messages?.length) {
      return;
    }

    return this.producer.publishBatch(topic, messages);
  }

  async subscribe(topic, handler, options = {}) {
    return this.consumer.subscribe(topic, handler, options);
  }
}

module.exports = RedpandaAdapter;
