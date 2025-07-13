import { Kafka, Consumer } from 'kafkajs';

export class KafkaConsumer {
  private kafka!: Kafka;
  private consumer!: Consumer;

  async createConnection(
    clientId: string = 'my-app-consumer',
    groupId: string = 'test-group',
    brokerAddress: string = process.env.KAFKA_BROKERS || 'localhost:29092,localhost:29093,localhost:29094'
  ) {
    this.kafka = new Kafka({
      clientId,
      brokers: brokerAddress.split(','),
      retry: {
        initialRetryTime: 100,
        retries: 8
      }
    });

    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 6000,
      heartbeatInterval: 1000
    });
  }

  async connect() {
    try {
      if (!this.consumer) {
        throw new Error('Consumer not initialized. Call createConnection first.');
      }
      console.log('Connecting to Kafka consumer...');
      await this.consumer.connect();
      console.log('Consumer connected successfully');
    } catch (error) {
      console.error('Error connecting consumer:', error);
      throw error;
    }
  }

  async subscribeToTopic(topic: string, callback: (message: any) => void, options: {
    fromBeginning?: boolean;
    manualCommit?: boolean;
  } = {}) {
    const { fromBeginning = true, manualCommit = false } = options;
    
    try {
      await this.consumer.subscribe({ topic, fromBeginning });

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value?.toString();
          console.log(`Received message from topic ${topic}: ${value}`);
          if (value && callback) {
            try {
              const parsedValue = JSON.parse(value);
              await callback(parsedValue);
            } catch {
              await callback(value);
            }
          }
        },
        autoCommit: !manualCommit,
        partitionsConsumedConcurrently: 1
      });

      this.consumer.on(this.consumer.events.GROUP_JOIN, ({ payload }) => {
        console.log('Consumer group joined:', payload);
      });

      this.consumer.on(this.consumer.events.REBALANCING, () => {
        console.log('Consumer group rebalancing...');
      });

      this.consumer.on(this.consumer.events.HEARTBEAT, () => {
      });

      console.log(`Subscribed to topic ${topic}`);
    } catch (error) {
      console.error('Error subscribing to topic:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      console.log('Starting consumer disconnect...');
      await this.consumer.disconnect();
      console.log('Consumer disconnected');
    } catch (error) {
      console.log('Consumer was not connected, skipping disconnect');
    }
  }

  getConsumer(): Consumer {
    return this.consumer;
  }
}
