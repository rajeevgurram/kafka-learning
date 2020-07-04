package com.github.rajeevgurram.kafka.tutorial_01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class ProducerDemoWithKeys {
    final KafkaProducer<String, String> producer;

    @Autowired
    public ProducerDemoWithKeys(final KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    @PostConstruct
    public void sendData() {
        for(int i = 0; i < 1000000; i ++) {
            // Same key records goes to same partition
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "my_key", "from java " + i);
            producer.send(record);
        }
        producer.flush();
    }
}
