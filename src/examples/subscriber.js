require("dotenv").config();

const fs = require("fs");

const path = require("path");

const { OrbitStream } = require("../../index");

const StorageFactory = require("@orbit-stream/storage");

const storage = StorageFactory.create();

async function main() {
  // ==================================================
  // REDPANDA
  // ==================================================

  const stream = new OrbitStream({
    clientId: "consumer",

    groupId: `telemetry-test-${Date.now()}`,

    brokers: ["localhost:9092"],

    maxBytesPerPartition: 50 * 1024 * 1024,

    maxBytes: 200 * 1024 * 1024,

    minBytes: 4 * 1024 * 1024,

    maxWaitTimeInMs: 250,
  });

  await stream.connect();

  console.log("telemetry archiver connected");

  // ==================================================
  // CONFIG
  // ==================================================

  const TMP_DIR = "/tmp/telemetry";

  const MAX_CHUNK_SIZE = 500 * 1024 * 1024; // 500 MB

  const MAX_CHUNK_DURATION = 30 * 1000; // 30 sec

  const MAX_UPLOAD_QUEUE = 50;

  // ==================================================
  // INIT TMP DIR
  // ==================================================

  fs.mkdirSync(TMP_DIR, {
    recursive: true,
  });

  // ==================================================
  // PARTITION WRITERS
  // ==================================================

  const partitionWriters = new Map();

  // ==================================================
  // UPLOAD QUEUE
  // ==================================================

  const uploadQueue = [];

  let uploading = false;

  // ==================================================
  // UPLOAD WORKER
  // ==================================================

  async function uploadWorker() {
    if (uploading) {
      return;
    }

    uploading = true;

    while (true) {
      const item = uploadQueue.shift();

      if (!item) {
        await sleep(1000);

        continue;
      }

      try {
        console.log(`uploading ${item.objectKey}`);

        await storage.upload({
          bucket: process.env.S3_BUCKET,

          key: item.objectKey,

          body: fs.createReadStream(item.filePath),

          contentType: "application/octet-stream",

          metadata: {
            partition: String(item.metadata.partition),

            startOffset: String(item.metadata.startOffset),

            endOffset: String(item.metadata.endOffset),

            size: String(item.metadata.size),
          },
        });

        fs.unlinkSync(item.filePath);

        console.log(`uploaded ${item.objectKey}`);
      } catch (error) {
        console.error(`upload failed ${item.objectKey}`, error);

        uploadQueue.push(item);

        await sleep(5000);
      }
    }
  }

  // ==================================================
  // ROTATE WRITER
  // ==================================================

  async function rotateWriter(partition, writer) {
    if (!writer || writer.rotating) {
      return;
    }

    writer.rotating = true;

    const oldWriter = writer;

    createWriter(partition);

    finalizeWriter(oldWriter).catch(console.error);
  }

  // ==================================================
  // START WORKER
  // ==================================================

  uploadWorker().catch(console.error);

  // ==================================================
  // SLEEP
  // ==================================================

  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  // ==================================================
  // CREATE WRITER
  // ==================================================

  function createWriter(partition) {
    const timestamp = Date.now();

    const filePath = path.join(
      TMP_DIR,
      `partition-${partition}-${timestamp}.bin`,
    );

    const streamWriter = fs.createWriteStream(filePath, {
      highWaterMark: 64 * 1024 * 1024,
    });

    const writer = {
      partition,

      filePath,

      streamWriter,

      currentSize: 0,

      chunkStartTime: Date.now(),

      startOffset: null,

      endOffset: null,

      rotating: false,
    };

    partitionWriters.set(partition, writer);

    console.log("created writer:", filePath);

    return writer;
  }

  // ==================================================
  // GET WRITER
  // ==================================================

  function getWriter(partition) {
    let writer = partitionWriters.get(partition);

    if (!writer) {
      writer = createWriter(partition);
    }

    return writer;
  }

  // ==================================================
  // FINALIZE WRITER
  // ==================================================

  async function finalizeWriter(writer) {
    while (uploadQueue.length >= MAX_UPLOAD_QUEUE) {
      console.warn("upload queue full, waiting...");

      await sleep(1000);
    }

    return new Promise((resolve, reject) => {
      writer.streamWriter.end(async () => {
        try {
          const date = new Date();

          const objectKey =
            `telemetry/year=${date.getUTCFullYear()}` +
            `/month=${String(date.getUTCMonth() + 1).padStart(2, "0")}` +
            `/day=${String(date.getUTCDate()).padStart(2, "0")}` +
            `/partition=${writer.partition}` +
            `/${writer.startOffset}-${writer.endOffset}.bin`;

          uploadQueue.push({
            filePath: writer.filePath,

            objectKey,

            metadata: {
              partition: writer.partition,

              startOffset: writer.startOffset,

              endOffset: writer.endOffset,

              size: writer.currentSize,

              createdAt: new Date().toISOString(),
            },
          });

          partitionWriters.delete(writer.partition);

          resolve();
        } catch (error) {
          reject(error);
        }
      });
    });
  }

  // ==================================================
  // TIMER ROTATION
  // ==================================================

  setInterval(() => {
    for (const [partition, writer] of partitionWriters) {
      if (writer.rotating) {
        continue;
      }

      const elapsed = Date.now() - writer.chunkStartTime;

      if (elapsed >= MAX_CHUNK_DURATION) {
        writer.rotating = true;

        console.log(`rotating partition ${partition} by time`);

        // Create replacement FIRST
        createWriter(partition);

        // Then finalize old writer
        finalizeWriter(writer).catch(console.error);
      }
    }
  }, 1000);

  // ==================================================
  // STATS
  // ==================================================

  let secondMessages = 0;

  let secondBytes = 0;

  setInterval(() => {
    const mbps = (secondBytes * 8) / 1024 / 1024;

    console.log({
      messagesPerSec: secondMessages,

      throughputMbps: mbps.toFixed(2),

      uploadQueue: uploadQueue.length,
    });

    secondMessages = 0;

    secondBytes = 0;
  }, 1000);

  // ==================================================
  // SUBSCRIBE
  // ==================================================

  console.log("before subscribe");

  await stream.subscribe(
    "telemetry-topic",

    async (messages) => {
      for (const msg of messages) {
        const partition = msg.partition ?? 0;

        const offset = msg.offset ?? null;

        const value = msg.value;

        if (!value) {
          continue;
        }

        const writer = getWriter(partition);

        // ==========================
        // METADATA
        // ==========================

        if (writer.startOffset === null) {
          writer.startOffset = offset;
        }

        writer.endOffset = offset;

        // ==========================
        // WRITE TO LOCAL FILE
        // ==========================

        const ok = writer.streamWriter.write(value);

        writer.currentSize += value.length;

        secondMessages++;

        secondBytes += value.length;

        // ==========================
        // BACKPRESSURE
        // ==========================

        if (!ok) {
          await new Promise((resolve) =>
            writer.streamWriter.once("drain", resolve),
          );
        }

        // ==========================
        // SIZE ROTATION
        // ==========================
        if (writer.currentSize >= MAX_CHUNK_SIZE && !writer.rotating) {
          console.log(`rotating partition ${partition} by size`);

          rotateWriter(partition, writer);
        }
      }
    },

    {
      fromBeginning: false,

      partitionsConsumedConcurrently: 8,
    },
  );

  console.log("consumer subscribed");
}

// process.on("SIGINT", async () => {
//   console.log("gracefully shutting down...");

//   for (const [, writer] of partitionWriters) {
//     try {
//       await finalizeWriter(writer);
//     } catch (error) {
//       console.error(error);
//     }
//   }

//   process.exit(0);
// });

main().catch(console.error);
