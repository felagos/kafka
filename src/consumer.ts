import { KafkaConsumer } from "./kafka-consumer";

const runConsumer = async () => {
    const kafkaConnection = new KafkaConsumer();

    try {
        await kafkaConnection.createConnection();

        kafkaConnection.subscribeToTopic('test-topic', (message) => {
            console.log('Received message:', JSON.stringify(message, null, 2));
        })

        await kafkaConnection.disconnect();

        process.exit(0);
    } catch (error) {
        console.error('Consumer failed:', error);
        process.exit(1);
    } finally {
        await kafkaConnection.disconnect();
    }

}

runConsumer();