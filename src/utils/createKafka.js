const { Kafka } = require("kafkajs");

function createKafka(config = {}) {
  return new Kafka({
    clientId: config.clientId || "orbit-stream",

    brokers: config.brokers || ["localhost:9092"],

    ssl: config.ssl || false,

    sasl: config.sasl,

    retry: {
      retries: 10,
    },

    connectionTimeout: 30000,

    requestTimeout: 30000,
  });
}

module.exports = createKafka;
