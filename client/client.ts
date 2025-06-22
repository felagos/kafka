import { connectAll, sendMessage, subscribeToTopic, disconnectAll } from './kafka-client.ts';

async function main() {
  try {
    // Connect to Kafka
    await connectAll();
    
    // Define the topic
    const topic = 'test-topic';
    
    // Subscribe to the topic
    await subscribeToTopic(topic, (message) => {
      console.log('Processed message:', message);
    });
    
    // Send a test message
    setTimeout(async () => {
      await sendMessage(topic, { text: 'Hello Kafka!', timestamp: new Date().toISOString() });
    }, 1000);
    
    // Keep the application running for a while to receive messages
    console.log('Waiting for messages...');
    
    // Disconnect after 10 seconds (just for this example)
    setTimeout(async () => {
      console.log('Disconnecting...');
      await disconnectAll();
      process.exit(0);
    }, 10000);
    
  } catch (error) {
    console.error('Error in main function:', error);
    process.exit(1);
  }
}

main();
