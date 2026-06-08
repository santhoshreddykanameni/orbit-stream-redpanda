const { OrbitStream } = require("@orbit-stream/redpanda");
async function main() {
  const stream = new OrbitStream({
    brokers: ["localhost:9092"],
    groupId: "test-group",
  });
  await stream.connect();
  console.log("consumer connected");
  await stream.subscribe("telemetry", async (messages) => {
    for (let i = 0; i < messages.length; i++) {
      console.log(messages[i].value.toString());
    }
  });
}
main().catch(console.error);
