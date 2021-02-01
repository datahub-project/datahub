package com.linkedin.datahub.graphql.service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.data.elasticsearch.ElasticsearchAutoConfiguration;
import org.springframework.boot.autoconfigure.elasticsearch.rest.RestClientAutoConfiguration;

@SuppressWarnings("checkstyle:HideUtilityClassConstructor")
@SpringBootApplication(exclude = {ElasticsearchAutoConfiguration.class, RestClientAutoConfiguration.class})
public class GmsGraphQLApplication {
    public static void main(String[] args) {
        SpringApplication.run(GmsGraphQLApplication.class, args);
    }
}
