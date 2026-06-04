import { BaseAdapter, Message } from "@orbit-stream/core";

export interface RedpandaConfig {
  clientId?: string;

  brokers?: string[];

  ssl?: boolean;

  sasl?: any;

  groupId?: string;

  batchSize?: number;

  flushInterval?: number;

  maxInFlightRequests?: number;
}

export class RedpandaAdapter extends BaseAdapter {
  constructor(config?: RedpandaConfig);

  connect(): Promise<void>;

  disconnect(): Promise<void>;

  publish(topic: string, message: Message): Promise<void>;

  publishBatch(topic: string, messages: Message[]): Promise<void>;

  subscribe(
    topic: string,
    handler: (messages: Message[]) => Promise<void> | void,
    options?: {
      fromBeginning?: boolean;
      partitionsConsumedConcurrently?: number;
    },
  ): Promise<void>;
}
