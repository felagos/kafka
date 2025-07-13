import { KafkaConsumer } from "./kafka-consumer";

const runConsumer = async () => {
    const kafkaConnection = new KafkaConsumer();

    try {
        await kafkaConnection.createConnection();
         await kafkaConnection.connect();

        await kafkaConnection.subscribeToTopic('test-topic', async (message) => {
            console.log('Processing message:', JSON.stringify(message, null, 2));
        }, { fromBeginning: true, });

        await kafkaConnection.disconnect();
    } catch (error) {
        console.error('Consumer failed:', error);
    }

}

runConsumer();