import { Kafka, Consumer } from 'kafkajs';

export class KafkaConsumer {
  private kafka!: Kafka;
  private consumer!: Consumer;

  async createConnection(
    clientId: string = 'my-app-consumer',
    groupId: string = 'test-group',
    brokerAddress: string = process.env.KAFKA_BROKER || 'localhost:29092'
  ) {
    this.kafka = new Kafka({
      clientId,
      brokers: [brokerAddress],
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

  async subscribeToTopic(topic: string, callback: (message: any) => void) {
    try {
      await this.consumer.subscribe({ topic, fromBeginning: true });

      await this.consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
          const value = message.value?.toString();
          console.log(`Received message from topic ${topic}: ${value}`);
          if (value && callback) {
            try {
              const parsedValue = JSON.parse(value);
              callback(parsedValue);
            } catch {
              callback(value);
            }
          }
        },
        autoCommit: true,
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
