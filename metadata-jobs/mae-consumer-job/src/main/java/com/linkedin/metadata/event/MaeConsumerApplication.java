package com.linkedin.metadata.event;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {RestClientAutoConfiguration.class})
public class MaeConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(MaeConsumerApplication.class, args);
    }

}
