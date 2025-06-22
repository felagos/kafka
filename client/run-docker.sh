#!/bin/bash

# Build and run the Kafka client Docker container

# Make sure Docker is running
echo "Checking Docker status..."
docker info > /dev/null 2>&1
if [ $? -ne 0 ]; then
  echo "Docker is not running. Please start Docker and try again."
  exit 1
fi

# Build the Docker image
echo "Building Docker image for Kafka client..."
docker-compose build

# Run the container
echo "Starting Kafka client container..."
docker-compose up

echo "Done!"
