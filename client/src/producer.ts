import { KafkaProducer } from "./kafka-producer";

const kafkaConnection = new KafkaProducer();

await kafkaConnection.createConnection();

await kafkaConnection.createTopicIfNotExists('test-topic');

await kafkaConnection.sendMessage('test-topic', {
    key: 'value',
    timestamp: new Date().toISOString(),
    message: 'Hello from producer!'
});

await kafkaConnection.disconnect();