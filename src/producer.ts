import { KafkaProducer } from "./kafka-producer";

async function runProducer() {
  const kafkaConnection = new KafkaProducer();

  try {
    console.log('Starting Kafka producer...');
    
    await kafkaConnection.createConnection();
    
    await kafkaConnection.createTopicIfNotExists('test-topic');

    await kafkaConnection.sendMessage('test-topic', {
        key: 'value',
        timestamp: new Date().toISOString(),
        message: 'Hello from producer!'
    });

    console.log('Producer completed successfully');
  } catch (error) {
    console.error('Producer failed:', error);
    process.exit(1);
  } finally {
    await kafkaConnection.disconnect();
  }
}

runProducer();