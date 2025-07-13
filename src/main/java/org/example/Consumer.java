package org.example;

import java.time.Duration;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class Consumer {
    public static void main(String[] args) {
        var consumerConnection = new KafkaConsumerConnection("demo-group");
        
        consumerConnection.subscribe("demo_java");
        
        System.out.println("Starting consumer... Press Ctrl+C to stop");
        
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumerConnection.pollMessages(Duration.ofMillis(1000));
                
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        System.out.println("Raw JSON received: " + record.value());
                        Message message = consumerConnection.deserializeMessage(record.value(), Message.class);
                        
                        System.out.printf("Consumed message: Topic=%s, Partition=%d, Offset=%d, Key=%s%n",
                                record.topic(), record.partition(), record.offset(), record.key());
                        System.out.printf("Message Title: %s%n", message.getTitle());
                        System.out.printf("Message Content: %s%n", message.getContent());
                        System.out.println("---");
                        
                    } catch (Exception e) {
                        System.err.println("Error processing message: " + e.getMessage());
                        System.err.println("Raw message: " + record.value());
                        e.printStackTrace();
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Consumer error: " + e.getMessage());
        } finally {
            consumerConnection.close();
            System.out.println("Consumer closed.");
        }
    }
}
