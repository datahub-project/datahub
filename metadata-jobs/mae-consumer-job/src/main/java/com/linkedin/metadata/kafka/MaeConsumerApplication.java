package com.linkedin.metadata.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {RestClientAutoConfiguration.class, KafkaAutoConfiguration.class})
public class MaeConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MaeConsumerApplication.class, args);
    }

}
