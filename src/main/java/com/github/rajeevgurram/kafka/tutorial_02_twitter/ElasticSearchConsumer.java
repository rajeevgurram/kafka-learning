package com.github.rajeevgurram.kafka.tutorial_02_twitter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.time.Duration;

@Component
public class ElasticSearchConsumer {
    private final KafkaConsumer<String, String> consumer;
    private final RestHighLevelClient elasticSearchClient;

    @Autowired
    public ElasticSearchConsumer(@Qualifier("twitter") final KafkaConsumer<String, String> consumer,
                                 @Qualifier("elasticSearchClient") final RestHighLevelClient elasticSearchClient) {
        this.consumer = consumer;
        this.elasticSearchClient = elasticSearchClient;
    }

    @PostConstruct
    public void run() {
        new Thread(() -> {
            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record : records) {
                    // Store the record in ElasticSearch
                    storeTweetInElasticSearch(record);
                }
            }
        }).start();
    }

    public void storeTweetInElasticSearch(final ConsumerRecord<String, String> tweet) {
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "_doc"
        ).source(tweet.value(), XContentType.JSON);

        try {
            IndexResponse response = elasticSearchClient.index(indexRequest, RequestOptions.DEFAULT);
            System.out.println("Elastic Search Response: " + response.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
