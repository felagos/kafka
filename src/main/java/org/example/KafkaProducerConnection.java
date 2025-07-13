package org.example;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

public class KafkaProducerConnection {

    final private String bootstrapServers = "localhost:29092,localhost:29093,localhost:29094";
    final private Producer<String, String> producer;
    final private ObjectMapper objectMapper;
    final private AdminClient adminClient;

    public KafkaProducerConnection() {
        producer = new KafkaProducer<>(this.getProperties());
        objectMapper = new ObjectMapper();
        this.adminClient = AdminClient.create(this.getAdminProperties());
    }

    private Properties getProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    private Properties getAdminProperties() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092,localhost:29093,localhost:29094");
        return properties;
    }

    public void createTopicIfNotExist(String topicName, int partitions, short replicationFactor) {
        try {
            ListTopicsResult existingTopics = adminClient.listTopics();
            if (!existingTopics.names().get().contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);
                CreateTopicsResult result = adminClient.createTopics(Collections.singletonList(newTopic));
                result.all().get();

                System.out.println("Topic '" + topicName + "' created successfully with " +
                        partitions + " partitions and replication factor " + replicationFactor);

                return;
            }
            System.out.println("Topic '" + topicName + "' already exists");
        } catch (TopicExistsException e) {
            System.out.println("Topic '" + topicName + "' already exists");
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Failed to create topic: " + topicName, e);
        }
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
