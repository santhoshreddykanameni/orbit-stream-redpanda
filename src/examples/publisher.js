const { OrbitStream } = require("../../index");

/*
 * Change this value:
 * 100, 200, 300, 400, 500
 */
const DATA_RATE_MBPS = 200;

const PACKET_SIZE = 8010;

const PACKETS_PER_SECOND = Math.floor((DATA_RATE_MBPS * 125000) / PACKET_SIZE);

const BATCH_SIZE = 5000;

const TOPIC = "telemetry-topic";

function createPacket(sequence) {
  const buffer = Buffer.alloc(PACKET_SIZE);

  buffer.writeUInt32BE(sequence, 0);

  return {
    key: `NETRA-${sequence}`,

    value: buffer,

    headers: {
      source: "load-test",
    },
  };
}

async function main() {
  const stream = new OrbitStream({
    clientId: `publisher-${DATA_RATE_MBPS}`,

    brokers: ["localhost:9092"],

    batchSize: BATCH_SIZE,
    topics: [
      {
        name: "telemetry-topic",
        partitions: 5,
        retention: {
          days: 7,
          maxSizeGB: 500,
        },
      },
    ],

    maxBatchBytes: 4 * 1024 * 1024,

    flushInterval: 100,
  });

  await stream.connect();

  console.log(`Started ${DATA_RATE_MBPS} Mbps test`);

  console.log(`Packet Size      : ${PACKET_SIZE} bytes`);

  console.log(`Packets / Second : ${PACKETS_PER_SECOND}`);

  let sequence = 0;

  let bytesSent = 0;

  setInterval(async () => {
    try {
      const messages = [];

      for (let i = 0; i < PACKETS_PER_SECOND; i++) {
        sequence++;

        messages.push(createPacket(sequence));

        bytesSent += PACKET_SIZE;
      }

      await stream.publishBatch(TOPIC, messages);
    } catch (error) {
      console.error("Publish failed:", error);
    }
  }, 1000);

  setInterval(() => {
    console.log({
      targetMbps: DATA_RATE_MBPS,

      actualMbps: Number(((bytesSent * 8) / 1024 / 1024).toFixed(2)),

      packetsPerSecond: PACKETS_PER_SECOND,

      totalPackets: sequence,
    });

    bytesSent = 0;
  }, 1000);
}

main().catch(console.error);
