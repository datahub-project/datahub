package com.linkedin.gms.factory.common;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Slf4j
@Configuration
public class RestHighLevelClientFactory {

  @Value("${ELASTICSEARCH_HOST:localhost}")
  private String host;

  @Value("${ELASTICSEARCH_PORT:9200}")
  private Integer port;

  @Value("${ELASTICSEARCH_THREAD_COUNT:1}")
  private Integer threadCount;

  @Value("${ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT:0}")
  private Integer connectionRequestTimeout;

  @Value("${ELASTICSEARCH_USE_SSL:false}")
  private boolean useSSL;

  @Autowired
  @Qualifier("elasticSearchSSLContext")
  private SSLContext sslContext;

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  protected RestHighLevelClient createInstance() {
    try {
      RestClient restClient;
      if (useSSL) {
        restClient = loadRestHttpsClient(host, port, threadCount, connectionRequestTimeout, sslContext);
      } else {
        restClient = loadRestHttpClient(host, port, threadCount, connectionRequestTimeout);
      }

      return new RestHighLevelClient(restClient);
    } catch (Exception e) {
      throw new RuntimeException("Error: RestClient is not properly initialized. " + e.toString());
    }
  }

  @Nonnull
  private static RestClient loadRestHttpClient(@Nonnull String host, int port, int threadCount,
                                               int connectionRequestTimeout) {
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder ->
                    httpAsyncClientBuilder.setDefaultIOReactorConfig(IOReactorConfig.custom()
                            .setIoThreadCount(threadCount).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
            setConnectionRequestTimeout(connectionRequestTimeout));

    return builder.build();
  }

  @Nonnull
  private static RestClient loadRestHttpsClient(@Nonnull String host, int port, int threadCount,
                                                int connectionRequestTimeout, @Nonnull SSLContext sslContext) {

    final RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "https"))
            .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setSSLContext(sslContext)
                    .setSSLHostnameVerifier(new NoopHostnameVerifier())
                    .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
            setConnectionRequestTimeout(connectionRequestTimeout));

    return builder.build();
  }
}

