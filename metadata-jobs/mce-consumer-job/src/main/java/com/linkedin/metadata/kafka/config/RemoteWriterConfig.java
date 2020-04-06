package com.linkedin.metadata.kafka.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.linkedin.metadata.dao.internal.BaseRemoteWriterDAO;
import com.linkedin.metadata.dao.internal.RestliRemoteWriterDAO;
import com.linkedin.metadata.restli.DefaultRestliClientFactory;
import com.linkedin.restli.client.Client;

@Configuration
public class RemoteWriterConfig {

    @Value("${GMS_HOST:localhost}")
    private String gmsHost;
    @Value("${GMS_PORT:8080}")
    private int gmsPort;

    @Bean
    public BaseRemoteWriterDAO remoteWriterDAO() {
        Client restClient = DefaultRestliClientFactory.getRestLiClient(gmsHost, gmsPort);
        return new RestliRemoteWriterDAO(restClient);
    }
}
