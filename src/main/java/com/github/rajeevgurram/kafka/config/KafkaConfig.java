package com.github.rajeevgurram.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Properties;

@Configuration
public class KafkaConfig {
    private final String bootstrapServer = "127.0.0.1:9092";
    private final String topic = "first_topic";

    @Bean
    // 3 steps to create producer
    public KafkaProducer<String, String> getKafkaProducer() {
        // Step1: Create producer properties.
        Properties properties = new Properties();

        properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step2: Create producer
        KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(properties);

        //Step3: Use this to send data to kafka

        return producer;
    }

    @Bean
    // 4 Steps to create kafka consumer
    public KafkaConsumer<String, String> getKafkaConsumer() {
        // Step1: Create consumer properties.
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka-learning");

        // Step2: Create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Step3: Subscribe to topics
        consumer.subscribe(Collections.singleton(topic));

        // Step4: Poll

        return consumer;
    }
}
