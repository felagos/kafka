package org.example;

public class Producer {
    public static void main(String[] args) {
        var topicName = "demo_java";
        var partitions = 3;
        var replicationFactor = (short) 2;

        var message = new Message("Hello", "This is a test message");
        var producerConnection = new KafkaProducerConnection();

        try {
            producerConnection.createTopicIfNotExist(topicName, partitions, replicationFactor);
            producerConnection.sendMessage(topicName, message);

            System.out.println("Message sent successfully!");
        } finally {
            producerConnection.close();
        }
    }
}