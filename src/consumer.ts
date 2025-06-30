import { KafkaConsumer } from "./kafka-consumer";

const runConsumer = async () => {
    const kafkaConnection = new KafkaConsumer();

    try {
        await kafkaConnection.createConnection();

        await kafkaConnection.subscribeToTopic('test-topic', async (message) => {
            console.log('Processing message:', JSON.stringify(message, null, 2));
            // Process your message here
            // The offset will only be committed after this callback completes successfully
        }, { fromBeginning: true, manualCommit: true });

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