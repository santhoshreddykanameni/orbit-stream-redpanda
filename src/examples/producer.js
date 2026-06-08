const { OrbitStream } = require("@orbit-stream/redpanda");
async function main() {
  const stream = new OrbitStream({
    brokers: ["localhost:9092"],
    clientId: "test-producer",
    batchSize: 1000,
    flushInterval: 100,
  });
  await stream.connect();
  console.log("producer connected");
  let counter = 0;
  setInterval(async () => {
    const payload = Buffer.from(`packet-${counter++}`);
    await stream.publish("telemetry", { value: payload });
  }, 100);
}
main().catch(console.error);
