package org.example;

public class Main {
    public static void main(String[] args) {
        var message = new Message("Hello", "This is a test message");
        
        var producerConnection = new KafkaProducerConnection();
        producerConnection.sendMessage("demo_java", message);
    }
}