package com.github.rajeevgurram.kafka.app.config;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Configuration
public class TwitterConfig {
    public final BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);

    private final String twitterConsumerKey;
    private final String twitterConsumerSecret;
    private final String twitterAccessToken;
    private final String twitterAccessTokenSecret;

    public TwitterConfig(
            @Value("${twitter.consumer.key}") final String twitterConsumerKey,
            @Value("${twitter.consumer.secret}") final String twitterConsumerSecret,
            @Value("${twitter.access_token}") final String twitterAccessToken,
            @Value("${twitter.access_token.secret}") final String twitterAccessTokenSecret) {
        this.twitterConsumerKey = twitterConsumerKey;
        this.twitterConsumerSecret = twitterConsumerSecret;
        this.twitterAccessToken = twitterAccessToken;
        this.twitterAccessTokenSecret = twitterAccessTokenSecret;
    }

    public BlockingQueue<String> getTwitterMessageQueue() {
        return msgQueue;
    }

    @Bean
    public Client getTwitterClient() {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("blm", "independence", "july4", "usa");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth =
                new OAuth1(
                        twitterConsumerKey,
                        twitterConsumerSecret,
                        twitterAccessToken,
                        twitterAccessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        Client hosebirdClient = builder.build();

        return hosebirdClient;
    }
}
