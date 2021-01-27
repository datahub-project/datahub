package com.linkedin.datahub.graphql.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchAutoConfiguration.class, RestClientAutoConfiguration.class})
public class DatahubGmsGraphQLApiApplication {
    public static void main(String[] args) {
        SpringApplication.run(DatahubGmsGraphQLApiApplication.class, args);
    }
}
