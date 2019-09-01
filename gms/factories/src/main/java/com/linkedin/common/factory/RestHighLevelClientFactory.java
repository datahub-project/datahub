package com.linkedin.common.factory;

import javax.annotation.Nonnull;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;


@Slf4j
@Configuration
@PropertySource("classpath:gms.properties")
public class RestHighLevelClientFactory {

  @Autowired
  private Environment env;

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  protected RestHighLevelClient createInstance() {
    try {
      RestClient restClient = loadRestHttpClient(env.getRequiredProperty("elasticsearch.hosts").split(","),
              Integer.valueOf(env.getRequiredProperty("elasticsearch.port")),
              Integer.valueOf(env.getProperty("elasticsearch.threadCount", "1")),
              Integer.valueOf(env.getProperty("elasticsearch.connectionRequestTimeout", "0")));

      return new RestHighLevelClient(restClient);
    } catch (Exception e) {
      throw new RuntimeException("Error: RestClient is not properly initialized. " + e.toString());
    }
  }

  @Nonnull
  private static RestClient loadRestHttpClient(@Nonnull String[] hosts, int port, int threadCount,
                                               int connectionRequestTimeout) throws Exception {

    HttpHost[] httpHosts = new HttpHost[hosts.length];
    for (int h = 0; h < hosts.length; h++) {
      httpHosts[h] = new HttpHost(hosts[h], port, "http");
    }

    RestClientBuilder builder;

    builder = RestClient.builder(httpHosts).setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
        // Configure number of threads for clients
        .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
            setConnectionRequestTimeout(connectionRequestTimeout));

    return builder.build();
  }
}

