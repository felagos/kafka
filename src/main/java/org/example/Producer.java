package org.example;

public class Producer {
    public static void main(String[] args) {
        var message = new Message("Hello", "This is a test message");
        
        var producerConnection = new KafkaProducerConnection();
        try {
            producerConnection.sendMessage("demo_java", message);
            System.out.println("Message sent successfully!");
        } finally {
            producerConnection.close();
        }
    }
}