package com.github.rajeevgurram.kafka.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages ={
                "com.github.rajeevgurram.kafka.app",
                "com.github.rajeevgurram.kafka.tutorial_02_twitter"
        })
public class Main {
    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }
}
