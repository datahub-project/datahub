package react.analytics;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;

public class ElasticClientFactory {
    @Nonnull
    public static RestHighLevelClient createElasticClient(
        final Boolean useSSL,
        final String host,
        final Integer port,
        final Integer threadCount,
        final Integer connectionRequestTimeout,
        final String username,
        final String password,
        final SSLContext sslContext
    ) {
        RestClientBuilder restClientBuilder;
        if (useSSL) {
            restClientBuilder = loadRestHttpsClient(host, port, threadCount, connectionRequestTimeout, sslContext, username,
                    password);
        } else {
            restClientBuilder = loadRestHttpClient(host, port, threadCount, connectionRequestTimeout);
        }
        return new RestHighLevelClient(restClientBuilder);
    }

    @Nonnull
    private static RestClientBuilder loadRestHttpClient(@Nonnull String host,
                                                        int port,
                                                        int threadCount,
                                                        int connectionRequestTimeout) {
        RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder
                        .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

        builder.setRequestConfigCallback(
                requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeout));

        return builder;
    }

    @Nonnull
    private static RestClientBuilder loadRestHttpsClient(@Nonnull String host,
                                                         int port,
                                                         int threadCount,
                                                         int connectionRequestTimeout,
                                                         @Nonnull SSLContext sslContext, String username, String password) {

        final RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "https"));
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
