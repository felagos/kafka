import { Kafka, Producer, Admin, Partitioners } from 'kafkajs';

export class KafkaProducer {
  private kafka!: Kafka;
  private producer!: Producer;
  private admin!: Admin;

  async createConnection(
    clientId: string = 'my-app-producer',
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
  }

  async sendMessage<T>(topic: string, message: T) {
    try {
      await this.producer.send({
        topic,
        messages: [
          { value: typeof message === 'object' ? JSON.stringify(message) : String(message) },
        ],
      });
      console.log(`Message sent to topic ${topic}`);
    } catch (error) {
      console.error('Error sending message:', error);
      throw error;
    }
  }

  async disconnect() {
    try {
      console.log('Starting admin disconnect...');
      await this.admin.disconnect();
      console.log('Admin disconnected');

      console.log('Starting producer disconnect...');
      await this.producer.disconnect();
      console.log('Producer disconnected');
    } catch (error) {
      console.error('Error disconnecting from Kafka:', error);
      throw error;
    }
  }

  getProducer(): Producer {
    return this.producer;
  }

  getAdmin(): Admin {
    return this.admin;
  }
}
