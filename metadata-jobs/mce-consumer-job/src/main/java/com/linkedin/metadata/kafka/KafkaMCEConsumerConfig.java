package com.linkedin.metadata.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;


@Slf4j
@Configuration
public class KafkaMCEConsumerConfig {
    @Autowired
    @Qualifier("parseqEngineThreads")
    private int concurrency;

    @Bean(name = "kafkaEventConsumerConcurrency")
    @Primary
    protected int kafkaEventConsumerConcurrency() {
        return concurrency;
    }
}
