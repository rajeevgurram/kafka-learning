package com.github.rajeevgurram.kafka.tutorial_01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class ProducerDemo {
    final KafkaProducer<String, String> producer;

    @Autowired
    public ProducerDemo(final KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    //@PostConstruct
    public void sendData() {
        for(int i = 0; i < 1000000; i ++) {
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", "from java " + i);
            producer.send(record);
        }
        producer.flush();
    }
}
