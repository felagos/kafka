import { KafkaConsumer } from "./kafka-consumer";

const kafkaConnection = new KafkaConsumer();

await kafkaConnection.createConnection();

kafkaConnection.subscribeToTopic('test-topic', (message) => {
    console.log('Received message:', JSON.stringify(message, null, 2));
})

await kafkaConnection.disconnect();

process.exit(0);