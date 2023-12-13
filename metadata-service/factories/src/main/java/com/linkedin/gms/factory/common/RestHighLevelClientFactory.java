package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.auth.AwsRequestSigningApacheInterceptor;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import java.io.IOException;
import javax.annotation.Nonnull;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ssl.DefaultHostnameVerifier;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.util.PublicSuffixMatcherLoader;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.conn.NHttpClientConnectionManager;
import org.apache.http.nio.conn.NoopIOSessionStrategy;
import org.apache.http.nio.conn.SchemeIOSessionStrategy;
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.nio.reactor.IOReactorExceptionHandler;
import org.apache.http.ssl.SSLContexts;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.signer.Aws4Signer;

@Slf4j
@Configuration
@PropertySource(value = "classpath:/application.yml", factory = YamlPropertySourceFactory.class)
@Import({ElasticsearchSSLContextFactory.class})
public class RestHighLevelClientFactory {

  @Value("${elasticsearch.host}")
  private String host;

  @Value("${elasticsearch.port}")
  private Integer port;

  @Value("${elasticsearch.threadCount}")
  private Integer threadCount;

  @Value("${elasticsearch.connectionRequestTimeout}")
  private Integer connectionRequestTimeout;

  @Value("${elasticsearch.username}")
  private String username;

  @Value("${elasticsearch.password}")
  private String password;

  @Value("#{new Boolean('${elasticsearch.useSSL}')}")
  private boolean useSSL;

  @Value("${elasticsearch.pathPrefix}")
  private String pathPrefix;

  @Value("#{new Boolean('${elasticsearch.opensearchUseAwsIamAuth}')}")
  private boolean opensearchUseAwsIamAuth;

  @Value("${elasticsearch.region}")
  private String region;

  @Autowired
  @Qualifier("elasticSearchSSLContext")
  private SSLContext sslContext;

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  public RestHighLevelClient createInstance(RestClientBuilder restClientBuilder) {

    return new RestHighLevelClient(restClientBuilder);
  }

  @Bean
  public RestClientBuilder loadRestClient() {
    final RestClientBuilder builder = createBuilder(useSSL ? "https" : "http");

    builder.setHttpClientConfigCallback(
        httpAsyncClientBuilder -> {
          if (useSSL) {
            httpAsyncClientBuilder
                .setSSLContext(sslContext)
                .setSSLHostnameVerifier(new NoopHostnameVerifier());
          }
          try {
            httpAsyncClientBuilder.setConnectionManager(createConnectionManager());
          } catch (IOReactorException e) {
            throw new IllegalStateException(
                "Unable to start ElasticSearch client. Please verify connection configuration.");
          }
          httpAsyncClientBuilder.setDefaultIOReactorConfig(
              IOReactorConfig.custom().setIoThreadCount(threadCount).build());

          setCredentials(httpAsyncClientBuilder);

          return httpAsyncClientBuilder;
        });

    return builder;
  }

  @Nonnull
  private RestClientBuilder createBuilder(String scheme) {
    final RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, scheme));

    if (!StringUtils.isEmpty(pathPrefix)) {
      builder.setPathPrefix(pathPrefix);
    }

    builder.setRequestConfigCallback(
        requestConfigBuilder ->
            requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout));

    return builder;
  }

  /**
   * Needed to override ExceptionHandler behavior for cases where IO error would have put client in
   * unrecoverable state We don't utilize system properties in the client builder, so setting
   * defaults pulled from {@link HttpAsyncClientBuilder#build()}.
   *
   * @return
   */
  private NHttpClientConnectionManager createConnectionManager() throws IOReactorException {
    SSLContext sslContext = SSLContexts.createDefault();
    HostnameVerifier hostnameVerifier =
        new DefaultHostnameVerifier(PublicSuffixMatcherLoader.getDefault());
    SchemeIOSessionStrategy sslStrategy =
        new SSLIOSessionStrategy(sslContext, null, null, hostnameVerifier);

    IOReactorConfig ioReactorConfig =
        IOReactorConfig.custom().setIoThreadCount(threadCount).build();
    DefaultConnectingIOReactor ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
    IOReactorExceptionHandler ioReactorExceptionHandler =
        new IOReactorExceptionHandler() {
          @Override
          public boolean handle(IOException ex) {
            log.error("IO Exception caught during ElasticSearch connection.", ex);
            return true;
          }

          @Override
          public boolean handle(RuntimeException ex) {
            log.error("Runtime Exception caught during ElasticSearch connection.", ex);
            return true;
          }
        };
    ioReactor.setExceptionHandler(ioReactorExceptionHandler);

    return new PoolingNHttpClientConnectionManager(
        ioReactor,
        RegistryBuilder.<SchemeIOSessionStrategy>create()
            .register("http", NoopIOSessionStrategy.INSTANCE)
            .register("https", sslStrategy)
            .build());
  }

  private void setCredentials(HttpAsyncClientBuilder httpAsyncClientBuilder) {
    if (username != null && password != null) {
      final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
      credentialsProvider.setCredentials(
          AuthScope.ANY, new UsernamePasswordCredentials(username, password));
      httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
    }
    if (opensearchUseAwsIamAuth) {
      HttpRequestInterceptor interceptor = getAwsRequestSigningInterceptor(region);
      httpAsyncClientBuilder.addInterceptorLast(interceptor);
    }
  }

  private HttpRequestInterceptor getAwsRequestSigningInterceptor(String region) {

    if (region == null) {
      throw new IllegalArgumentException(
          "Region must not be null when opensearchUseAwsIamAuth is enabled");
    }
    Aws4Signer signer = Aws4Signer.create();
    // Uses default AWS credentials
    return new AwsRequestSigningApacheInterceptor(
        "es", signer, DefaultCredentialsProvider.create(), region);
  }
}
