package com.linkedin.gms.factory.common;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
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
import org.springframework.context.annotation.Import;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.auth.AuthScope;

@Slf4j
@Configuration
@Import({ ElasticsearchSSLContextFactory.class })
public class RestHighLevelClientFactory {

  @Value("${ELASTICSEARCH_HOST:localhost}")
  private String host;

  @Value("${ELASTICSEARCH_PORT:9200}")
  private Integer port;

  @Value("${ELASTICSEARCH_USERNAME:#{null}}")
  private String username;

  @Value("${ELASTICSEARCH_PASSWORD:#{null}}")
  private String password;

  @Value("${ELASTICSEARCH_THREAD_COUNT:1}")
  private Integer threadCount;

  @Value("${ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT:0}")
  private Integer connectionRequestTimeout;

  @Value("${ELASTICSEARCH_USE_SSL:false}")
  private boolean useSSL;

  @Value("${ELASTICSEARCH_PATH_PREFIX:#{null}}")
  private String pathPrefix;

  @Autowired
  @Qualifier("elasticSearchSSLContext")
  private SSLContext sslContext;

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  protected RestHighLevelClient createInstance() {
    RestClientBuilder restClientBuilder;

    if (useSSL) {
      restClientBuilder = loadRestHttpsClient(host, port, pathPrefix, threadCount, connectionRequestTimeout, sslContext, username,
          password);
    } else {
      restClientBuilder = loadRestHttpClient(host, port, pathPrefix, threadCount, connectionRequestTimeout);
    }

    return new RestHighLevelClient(restClientBuilder);
  }

  @Nonnull
  private static RestClientBuilder loadRestHttpClient(@Nonnull String host, int port, String pathPrefix, int threadCount,
      int connectionRequestTimeout) {
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
            .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

    if (!StringUtils.isEmpty(pathPrefix)) {
      builder.setPathPrefix(pathPrefix);
    }

    builder.setRequestConfigCallback(
        requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout));

    return builder;
  }

  @Nonnull
  private static RestClientBuilder loadRestHttpsClient(@Nonnull String host, int port, String pathPrefix, int threadCount,
      int connectionRequestTimeout, @Nonnull SSLContext sslContext, String username, String password) {

    final RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "https"));

    if (!StringUtils.isEmpty(pathPrefix)) {
      builder.setPathPrefix(pathPrefix);
    }

    builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
        httpAsyncClientBuilder.setSSLContext(sslContext).setSSLHostnameVerifier(new NoopHostnameVerifier())
            .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build());

        if (username != null && password != null) {
          final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
          httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }

        return httpAsyncClientBuilder;
      }
    });

    builder.setRequestConfigCallback(
        requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout));

    return builder;
  }

}
