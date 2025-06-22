import { connectAll, disconnectAll, sendMessage } from "./kafka-client";

const topic = 'test-topic';

async function main() {
    try {
        let counter = 1;
        console.log('Connecting to Kafka...');
        await connectAll();
        
        console.log('Starting to send messages...');
        // Send a test message every 2 seconds
        const intervalId = setInterval(async () => {
            try {
                await sendMessage(topic, { 
                    text: `Sending message: ${counter++}`, 
                    timestamp: new Date().toISOString() 
                });
                
                if(counter > 10) {
                    clearInterval(intervalId);
                    console.log('Disconnecting from Kafka...');
                    await disconnectAll();
                    console.log('Successfully disconnected.');
                }
            } catch (error) {
                console.error('Error sending message:', error);
                clearInterval(intervalId);
                await disconnectAll();
            }
        }, 2000);
        
    } catch (error) {
        console.error('Error in main function:', error);
        await disconnectAll().catch(e => console.error('Error disconnecting:', e));
    }
}

main().catch(error => {
    console.error('Unhandled error:', error);
    process.exit(1);
});