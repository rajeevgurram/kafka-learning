package com.github.rajeevgurram.kafka.tutorial_02_twitter;

import com.github.rajeevgurram.kafka.app.config.TwitterConfig;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.BlockingQueue;

@Component
public class TwitterProducer {
    private final KafkaProducer<String, String> producer;
    private final Client twitterClient;
    private final BlockingQueue<String> msgQueue;

    @Autowired
    public TwitterProducer(final KafkaProducer<String, String> producer,
                           final Client twitterClient,
                           final TwitterConfig twitterConfig) {
        this.producer = producer;
        this.twitterClient = twitterClient;
        this.msgQueue = twitterConfig.getTwitterMessageQueue();
    }

    @PostConstruct
    public void run() {
        twitterClient.connect();

        new Thread(() -> {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutting Down");
                twitterClient.stop();
                producer.close();
                System.out.println("Done");
            }));

            while (!twitterClient.isDone()) {
                String msg = null;
                try {
                    msg = msgQueue.take();
                    ProducerRecord<String, String> record = new ProducerRecord<>("twitter-tweets", msg);
                    producer.send(record, new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null) {
                                e.printStackTrace();
                            }
                        }
                    });
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    twitterClient.stop();
                }
            }
        }).start();
    }
}
