# Kafka TypeScript Client with Bun and Docker

A TypeScript client for connecting to Apache Kafka using Bun runtime, containerized with Docker.

## Prerequisites

- Docker and Docker Compose
- Kafka running (using the parent directory's docker-compose.yml)

## Development Setup

If you want to develop without Docker:

1. Install Bun:
   ```bash
   # Windows (PowerShell)
   powershell -c "irm bun.sh/install.ps1 | iex"
   
   # macOS/Linux
   curl -fsSL https://bun.sh/install | bash
   ```

2. Install dependencies:
   ```bash
   bun install
   ```

3. Run the example:
   ```bash
   bun start
   ```

## Docker Setup

### Building and Running with Docker

1. Make sure Kafka is running in the parent directory:
   ```bash
   cd ..
   docker-compose up -d
   ```

2. Build and run the client container:
   ```bash
   docker-compose up --build
   ```

This will:
1. Build the Docker image for the Kafka client
2. Start a container that connects to the Kafka broker
3. Run the example code that produces and consumes messages

### Environment Variables

The Kafka client supports the following environment variables:

- `KAFKA_BROKER`: The Kafka broker address (default: `localhost:29092`)

You can configure these in the docker-compose.yml file.

## Project Structure

- `kafka-client.ts`: The main Kafka client library
- `example.ts`: Example usage of the client
- `Dockerfile`: Container definition for the client
- `docker-compose.yml`: Docker Compose configuration for the client
