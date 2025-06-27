import { Kafka, Producer, Consumer } from 'kafkajs';

export class KafkaConnection {
  private kafka!: Kafka;
  private producer!: Producer;
  private consumer!: Consumer;

  createConnection(
    clientId: string = 'my-app',
    groupId: string = 'test-group',
    brokerAddress: string = process.env.KAFKA_BROKER || 'localhost:29092'
  ) {
    this.kafka = new Kafka({
      clientId,
      brokers: [brokerAddress],
    });

    this.producer = this.kafka.producer();
    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 30000,
      heartbeatInterval: 5000
    });

    return this;
  }

  async connectAll() {
    try {
      await this.producer.connect();
      console.log('Producer connected successfully');

      await this.consumer.connect();
      console.log('Consumer connected successfully');

      return { producer: this.producer, consumer: this.consumer };
    } catch (error) {
      console.error('Error connecting to Kafka:', error);
      throw error;
    }
  }

  async sendMessage(topic: string, message: any) {
    try {
      await this.producer.send({
        topic,
        messages: [
          { value: typeof message === 'string' ? message : JSON.stringify(message) },
        ],
      });
      console.log(`Message sent to topic ${topic}`);
    } catch (error) {
      console.error('Error sending message:', error);
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

  async disconnectAll() {
    try {
      console.log('Starting producer disconnect...');
      await this.producer.disconnect();
      console.log('Producer disconnected');

      console.log('Starting consumer disconnect...');
      await new Promise(resolve => setTimeout(resolve, 1000));
      await this.consumer.disconnect();
      console.log('Consumer disconnected');
    } catch (error) {
      console.error('Error disconnecting from Kafka:', error);
      throw error;
    }
  }

  getProducer(): Producer {
    return this.producer;
  }

  getConsumer(): Consumer {
    return this.consumer;
  }
}