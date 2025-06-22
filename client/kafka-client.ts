import { Kafka } from 'kafkajs';

// Get broker address from environment variable or use default
const brokerAddress = process.env.KAFKA_BROKER || 'localhost:29092';

// Configure the Kafka client with your broker's details
const kafka = new Kafka({
  clientId: 'my-app',
  brokers: [brokerAddress], // Use environment variable or default
});

// Create a producer
const producer = kafka.producer();

// Create a consumer with better rebalancing strategy
const consumer = kafka.consumer({ 
  groupId: 'test-group',
  sessionTimeout: 30000, // Increase session timeout to handle rebalancing better
  heartbeatInterval: 5000 // More frequent heartbeats
});

// Function to connect both producer and consumer
async function connectAll() {
  try {
    // Connect the producer
    await producer.connect();
    console.log('Producer connected successfully');
    
    // Connect the consumer
    await consumer.connect();
    console.log('Consumer connected successfully');
    
    return { producer, consumer };
  } catch (error) {
    console.error('Error connecting to Kafka:', error);
    throw error;
  }
}

// Function to send a message to a topic
async function sendMessage(topic: string, message: any) {
  try {
    await producer.send({
      topic,
      messages: [
        { value: typeof message === 'string' ? message : JSON.stringify(message) },
      ],
    });
    console.log(`Message sent to topic ${topic}`);
  } catch (error) {
    console.error('Error sending message:', error);
    throw error;
  }
}

// Function to subscribe to a topic and consume messages
async function subscribeToTopic(topic: string, callback: (message: any) => void) {
  try {
    // Subscribe to the topic
    await consumer.subscribe({ topic, fromBeginning: true });
    
    // Start consuming messages with rebalancing event handlers
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value?.toString();
        console.log(`Received message from topic ${topic}: ${value}`);
        if (value && callback) {
          try {
            const parsedValue = JSON.parse(value);
            callback(parsedValue);
          } catch {
            callback(value);
          }
        }
      },
      // Add rebalancing event handlers
      autoCommit: true,
      partitionsConsumedConcurrently: 1
    });
    
    // Handle rebalance events
    consumer.on(consumer.events.GROUP_JOIN, ({ payload }) => {
      console.log('Consumer group joined:', payload);
    });
    
    consumer.on(consumer.events.REBALANCING, () => {
      console.log('Consumer group rebalancing...');
    });
    
    consumer.on(consumer.events.HEARTBEAT, () => {
      // Uncomment for debugging if needed
      // console.log('Consumer heartbeat');
    });
    
    console.log(`Subscribed to topic ${topic}`);
  } catch (error) {
    console.error('Error subscribing to topic:', error);
    throw error;
  }
}

// Function to disconnect both producer and consumer
async function disconnectAll() {
  try {
    console.log('Starting producer disconnect...');
    await producer.disconnect();
    console.log('Producer disconnected');
    
    console.log('Starting consumer disconnect...');
    // Add a small delay to ensure producer disconnection completes
    await new Promise(resolve => setTimeout(resolve, 1000));
    await consumer.disconnect();
    console.log('Consumer disconnected');
  } catch (error) {
    console.error('Error disconnecting from Kafka:', error);
    throw error;
  }
}

export {
  connectAll,
  sendMessage,
  subscribeToTopic,
  disconnectAll,
  producer,
  consumer,
};
