package com.linkedin.gms.factory.common;

import com.amazonaws.auth.AWS4Signer;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
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
@Import({ElasticsearchSSLContextFactory.class})
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

  @Value("${ELASTICSEARCH_USE_AWS_AUTH:false}")
  private boolean useAwsAuth;

  @Value("${AWS_REGION:us-west-2}")
  private String region;

  @Autowired
  @Qualifier("elasticSearchSSLContext")
  private SSLContext sslContext;

  private static final AWSCredentialsProvider CREDENTIALS_PROVIDER = new DefaultAWSCredentialsProviderChain();
  private static final String SERVICE_NAME = "es";

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  protected RestHighLevelClient createInstance() {
    RestClientBuilder restClientBuilder;

    if (useSSL) {
      restClientBuilder =
          loadRestHttpsClient(host, port, threadCount, connectionRequestTimeout, sslContext, username, password);
    } else {
      restClientBuilder = loadRestHttpClient(host, port, threadCount, connectionRequestTimeout);
    }


    return new RestHighLevelClient(restClientBuilder);
  }

  @Nonnull
  private RestClientBuilder loadRestHttpClient(@Nonnull String host, int port, int threadCount,
      int connectionRequestTimeout) {
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> {
          HttpAsyncClientBuilder clientBuilder = httpAsyncClientBuilder.setDefaultIOReactorConfig(
              IOReactorConfig.custom().setIoThreadCount(threadCount).build());
          if(useAwsAuth) httpAsyncClientBuilder.addInterceptorLast(getAwsAuthInterceptor());
          return clientBuilder;
        });

    builder.setRequestConfigCallback(
        requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout));

    return builder;
  }

  @Nonnull
  private RestClientBuilder loadRestHttpsClient(@Nonnull String host, int port, int threadCount,
      int connectionRequestTimeout, @Nonnull SSLContext sslContext, String username, String password) {

    final RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "https"));
    builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
      public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
        httpAsyncClientBuilder.setSSLContext(sslContext)
            .setSSLHostnameVerifier(new NoopHostnameVerifier())
            .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build());

        if (username != null && password != null) {
          final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
          credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));
          httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }

        if(useAwsAuth) {
          httpAsyncClientBuilder.addInterceptorLast(getAwsAuthInterceptor());
        }

        return httpAsyncClientBuilder;
      }
    });

    builder.setRequestConfigCallback(
        requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout));

    return builder;
  }

  private HttpRequestInterceptor getAwsAuthInterceptor() {
    AWS4Signer signer = new AWS4Signer();
    signer.setServiceName(SERVICE_NAME);
    signer.setRegionName(region);
    return new AWSRequestSigningApacheInterceptor(SERVICE_NAME, signer, CREDENTIALS_PROVIDER);
  }
}
