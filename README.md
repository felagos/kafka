# Kafka with TypeScript Client

This project sets up a complete Kafka environment with ZooKeeper, Kafka broker, Kafka UI, and a TypeScript client using Bun.

## Components

- **ZooKeeper**: Manages Kafka broker coordination
- **Kafka**: Message broker
- **Kafka UI**: Web interface for managing Kafka
- **Kafka Client**: TypeScript client application using Bun runtime

## Getting Started

### Start the Kafka environment with Client

```bash
docker-compose up -d
```

This will start ZooKeeper, Kafka, Kafka UI, and the TypeScript client.

### Access Kafka UI

The Kafka UI is available at [http://localhost:8080](http://localhost:8080).

### Check client status

To check if the client is properly connected to Kafka, run:

```bash
# PowerShell
.\check-client.ps1

# Bash
bash check-client.sh
```

## Usage

The project includes:

- `kafka-client.ts`: The main Kafka client with connection, producer, and consumer functionality
- `example.ts`: An example showing how to connect, produce, and consume messages

### Running the example

```bash
bun start
```

This will:
1. Connect to Kafka
2. Subscribe to the "test-topic" topic
3. Send a test message to the topic
4. Receive and print the message
5. Disconnect after 10 seconds

### Using in your own code

```typescript
import { connectAll, sendMessage, subscribeToTopic, disconnectAll } from './kafka-client.ts';

// Connect to Kafka
await connectAll();

// Send a message
await sendMessage('my-topic', { data: 'Hello, Kafka!' });

// Consume messages
await subscribeToTopic('my-topic', (message) => {
  console.log('Received:', message);
});

// Disconnect when done
await disconnectAll();
```

## Configuration

The Kafka connection is configured to connect to the broker address based on the environment:
- In Docker: `kafka:9092` (internal network)
- Locally: `localhost:29092` (external access)

## Client Development

The client code is located in the `client` directory. To develop the client locally:

1. Install Bun:
   ```bash
   # Windows (PowerShell)
   powershell -c "irm bun.sh/install.ps1 | iex"
   ```

2. Navigate to the client directory and install dependencies:
   ```bash
   cd client
   bun install
   ```

3. Run the client:
   ```bash
   bun start
   ```

For more details on the client, see the README in the client directory.
