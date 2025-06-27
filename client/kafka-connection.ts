import { Kafka, Producer, Consumer, Admin, Partitioners } from 'kafkajs';

export class KafkaConnection {
  private kafka!: Kafka;
  private producer!: Producer;
  private consumer!: Consumer;
  private admin!: Admin;

  async createConnection(
    clientId: string = 'my-app',
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

    this.producer = this.kafka.producer({
      createPartitioner: Partitioners.LegacyPartitioner
    });
    this.consumer = this.kafka.consumer({
      groupId,
      sessionTimeout: 6000,
      heartbeatInterval: 1000
    });
    this.admin = this.kafka.admin();

    await this.connectProducerAndAdmin();
  }

  private async connectProducerAndAdmin() {
    try {
      console.log('Connecting to Kafka admin...');
      await this.admin.connect();
      console.log('Admin connected successfully');

      console.log('Connecting to Kafka producer...');
      await this.producer.connect();
      console.log('Producer connected successfully');

    } catch (error) {
      console.error('Error connecting to Kafka:', error);
      throw error;
    }
  }

  async createTopicIfNotExists(topicName: string, numPartitions: number = 1, replicationFactor: number = 1) {
    try {
      const topics = await this.admin.listTopics();
      if (!topics.includes(topicName)) {
        console.log(`Creating topic: ${topicName}`);
        await this.admin.createTopics({
          topics: [{
            topic: topicName,
            numPartitions,
            replicationFactor
          }]
        });
        console.log(`Topic ${topicName} created successfully`);
        await new Promise(resolve => setTimeout(resolve, 2000));
      } else {
        console.log(`Topic ${topicName} already exists`);
      }
    } catch (error) {
      console.error(`Error creating topic ${topicName}:`, error);
      throw error;
    }
  }

  async sendMessage<T>(topic: string, message: T) {
    try {
      await this.createTopicIfNotExists(topic);
      
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
      console.log('Starting admin disconnect...');
      await this.admin.disconnect();
      console.log('Admin disconnected');

      console.log('Starting producer disconnect...');
      await this.producer.disconnect();
      console.log('Producer disconnected');

      try {
        console.log('Starting consumer disconnect...');
        await this.consumer.disconnect();
        console.log('Consumer disconnected');
      } catch (error) {
        console.log('Consumer was not connected, skipping disconnect');
      }
    } catch (error) {
      console.error('Error disconnecting from Kafka:', error);
      throw error;
    }
  }

  async connectConsumer() {
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

  getProducer(): Producer {
    return this.producer;
  }

  getConsumer(): Consumer {
    return this.consumer;
  }
}