import { KafkaConnection } from "./kafka-connection";

const kafkaConnection = new KafkaConnection();

await kafkaConnection.createConnection();

await kafkaConnection.sendMessage('test-topic', {
    key: 'value',
    timestamp: new Date().toISOString(),
    message: 'Hello from producer!'
});

kafkaConnection.disconnectAll();