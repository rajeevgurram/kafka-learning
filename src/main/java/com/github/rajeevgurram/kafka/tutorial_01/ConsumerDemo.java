package com.github.rajeevgurram.kafka.tutorial_01;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.time.Duration;

@Component
public class ConsumerDemo {
    private final KafkaConsumer<String, String> consumer;

    @Autowired
    public ConsumerDemo(final KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }

    @PostConstruct
    public void init() {
        new Thread(() -> {
            System.out.println("Initializing Kafka Consumer");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String, String> record : records) {
                    System.out.println(Thread.currentThread().getName() + " -> " + record.key() + ": " + record.value());
                }
            }
        }).start();
    }
}
