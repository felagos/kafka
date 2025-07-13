package org.example;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class KafkaProducerConnection {

    final String bootstrapServers = "localhost:29092,localhost:29093,localhost:29094";
    final Producer<String, String> producer;
    private final ObjectMapper objectMapper;

    public KafkaProducerConnection() {
        producer = new KafkaProducer<>(this.getProperties());
        objectMapper = new ObjectMapper();
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public <T> void sendMessage(String topic, T message) {
        try {
            String jsonMessage = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, jsonMessage);
            this.producer.send(record);
            this.producer.flush();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize message to JSON", e);
        }
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }

}
