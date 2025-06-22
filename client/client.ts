import { connectAll, subscribeToTopic, disconnectAll } from './kafka-client.ts';

async function main() {
  try {
    console.log('Connecting to Kafka...');
    // Connect to Kafka
    await connectAll();
    
    // Define the topic
    const topic = 'test-topic';
    
    console.log(`Subscribing to topic: ${topic}`);
    // Subscribe to the topic
    await subscribeToTopic(topic, (message) => {
      console.log('Processed message:', message);
    });
    
    // Keep the application running and handle graceful shutdown
    console.log('Waiting for messages...');
    
    // Handle graceful shutdown
    process.on('SIGINT', async () => {
      console.log('Received SIGINT. Gracefully shutting down...');
      await disconnectAll();
      process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
      console.log('Received SIGTERM. Gracefully shutting down...');
      await disconnectAll();
      process.exit(0);
    });
    
  } catch (error) {
    console.error('Error in main function:', error);
    await disconnectAll().catch(e => console.error('Error disconnecting:', e));
    process.exit(1);
  }
}

main().catch(error => {
  console.error('Unhandled error:', error);
  process.exit(1);
});
