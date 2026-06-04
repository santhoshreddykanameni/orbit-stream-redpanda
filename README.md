# orbit-stream-redpanda

High-performance Redpanda transport adapter for Orbit Stream.

# @orbit-stream/redpanda

High-performance Redpanda transport adapter for Orbit Stream.

Optimized for:

- high-throughput telemetry
- binary payloads
- batching
- low-latency streaming
- backpressure-aware processing

---

# Installation

```bash id="ygt1m4"
npm install @orbit-stream/redpanda
```

---

# Usage

```js id="w9d4p7"
const { RedpandaAdapter } = require("@orbit-stream/redpanda");

const stream = new RedpandaAdapter({
  brokers: ["localhost:9092"],

  groupId: "telemetry-group",

  batchSize: 5000,

  flushInterval: 100,
});

await stream.connect();

await stream.publish("telemetry", {
  value: Buffer.from("hello"),
});

await stream.subscribe("telemetry", async (messages) => {
  console.log(messages.length);
});
```

---

# Features

- KafkaJS based
- Redpanda optimized
- Batch publishing
- Batch consumption
- Buffer-first architecture
- Backpressure-aware
- High throughput optimized
- Binary payload support

---

# License

MIT
