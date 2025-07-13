package org.example;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class KafkaConsumerConnection {

    final String bootstrapServers = "localhost:29092,localhost:29093,localhost:29094";
    final Consumer<String, String> consumer;
    private final ObjectMapper objectMapper;

    public KafkaConsumerConnection(String groupId) {
        consumer = new KafkaConsumer<>(this.getProperties(groupId));
        objectMapper = new ObjectMapper();
    }

    private Properties getProperties(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        return properties;
    }

    public void subscribe(String topic) {
        consumer.subscribe(Collections.singletonList(topic));
    }

    public <T> ConsumerRecords<String, String> pollMessages(Duration timeout) {
        return consumer.poll(timeout);
    }

    public <T> T deserializeMessage(String jsonMessage, Class<T> clazz) {
        try {
            return objectMapper.readValue(jsonMessage, clazz);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to deserialize message from JSON", e);
        }
    }

    public void close() {
        if (consumer != null) {
            consumer.close();
        }
    }
}
