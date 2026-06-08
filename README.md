# @orbit-stream/redpanda

High-performance Redpanda transport adapter for Orbit Stream.

Optimized for:

- high-throughput telemetry streaming
- binary payload processing
- batch publishing and consumption
- low-latency message delivery
- backpressure-aware streaming
- large-scale distributed consumers

---

# Installation

```bash
npm install @orbit-stream/redpanda
```

---

# Features

- KafkaJS based transport
- Redpanda optimized architecture
- Batch publishing support
- Batch consumption support
- Buffer-first design
- Backpressure-aware processing
- High-throughput optimized
- Binary payload support
- Transport-independent OrbitStream API

---

# Usage

```js
const { OrbitStream } = require("@orbit-stream/redpanda");

async function main() {
  const stream = new OrbitStream({
    brokers: ["localhost:9092"],

    groupId: "telemetry-group",

    batchSize: 5000,

    flushInterval: 100,
  });

  await stream.connect();

  await stream.publish("telemetry", {
    value: Buffer.from("hello"),
  });

  await stream.subscribe(
    "telemetry",

    async (messages) => {
      console.log(messages.length);
    },
  );
}

main();
```

---

# Producer Example

```js
const { OrbitStream } = require("@orbit-stream/redpanda");

const stream = new OrbitStream({
  brokers: ["localhost:9092"],

  clientId: "telemetry-producer",
});

await stream.connect();

await stream.publish("telemetry", {
  value: Buffer.from("payload"),
});
```

---

# Consumer Example

```js
const { OrbitStream } = require("@orbit-stream/redpanda");

const stream = new OrbitStream({
  brokers: ["localhost:9092"],

  groupId: "telemetry-group",
});

await stream.connect();

await stream.subscribe(
  "telemetry",

  async (messages) => {
    console.log("received:", messages.length);
  },

  {
    partitionsConsumedConcurrently: 8,
  },
);
```

---

# Recommended Configuration

## Standard Telemetry

```js
{
  batchSize: 5000,
  flushInterval: 100
}
```

---

## High Throughput Telemetry (~800 Mbps)

```js
{
  batchSize: 10000,
  flushInterval: 50,
  maxInFlightRequests: 10
}
```

---

# Architecture

```txt
Telemetry Source
        │
        ▼
OrbitStream Producer
        │
        ▼
Redpanda Cluster
        │
        ▼
Consumer Group
        │
        ▼
Telemetry Processors
```

---

# Package Architecture

Orbit Stream uses modular transport packages:

| Package                  | Purpose                         |
| ------------------------ | ------------------------------- |
| `@orbit-stream/core`     | Shared interfaces and utilities |
| `@orbit-stream/redpanda` | Redpanda transport              |
| `@orbit-stream/valkey`   | Valkey Streams transport        |

Applications install only the transport they use.

---

# License

MIT
