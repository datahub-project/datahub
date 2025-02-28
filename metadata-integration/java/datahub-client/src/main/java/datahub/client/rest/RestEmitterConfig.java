package datahub.client.rest;

import datahub.event.EventFormatter;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.core5.util.TimeValue;

@Value
@Builder
@Slf4j
public class RestEmitterConfig {

  public static final int DEFAULT_CONNECT_TIMEOUT_SEC = 10;
  public static final int DEFAULT_READ_TIMEOUT_SEC = 10;
  public static final String DEFAULT_AUTH_TOKEN = null;
  public static final String CLIENT_VERSION_PROPERTY = "clientVersion";

  @Builder.Default String server = "http://localhost:8080";

  Integer timeoutSec;
  @Builder.Default boolean disableSslVerification = false;

  @Builder.Default boolean disableChunkedEncoding = false;

  @Builder.Default int maxRetries = 0;

  @Builder.Default int retryIntervalSec = 10;

  @Builder.Default String token = DEFAULT_AUTH_TOKEN;

  @Builder.Default @NonNull Map<String, String> extraHeaders = Collections.EMPTY_MAP;

  @Builder.Default
  EventFormatter eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);

  HttpAsyncClientBuilder asyncHttpClientBuilder;

  public static class RestEmitterConfigBuilder {

    private String getVersion() {
      try (InputStream foo =
          this.getClass().getClassLoader().getResourceAsStream("client.properties")) {
        Properties properties = new Properties();
        properties.load(foo);
        return properties.getProperty(CLIENT_VERSION_PROPERTY, "unknown");
      } catch (Exception e) {
        log.warn("Unable to find a version for datahub-client. Will set to unknown", e);
        return "unknown";
      }
    }

    private HttpAsyncClientBuilder asyncHttpClientBuilder =
        HttpAsyncClients.custom()
            .setUserAgent("DataHub-RestClient/" + getVersion())
            .setDefaultRequestConfig(
                RequestConfig.custom()
                    .setConnectionRequestTimeout(
                        DEFAULT_CONNECT_TIMEOUT_SEC * 1000,
                        java.util.concurrent.TimeUnit.MILLISECONDS)
                    .setResponseTimeout(
                        DEFAULT_READ_TIMEOUT_SEC * 1000, java.util.concurrent.TimeUnit.MILLISECONDS)
                    .build())
            .setRetryStrategy(
                new DatahubHttpRequestRetryStrategy(
                    maxRetries$value, TimeValue.ofSeconds(retryIntervalSec$value)));

    public RestEmitterConfigBuilder with(Consumer<RestEmitterConfigBuilder> builderFunction) {
      builderFunction.accept(this);
      return this;
    }

    public RestEmitterConfigBuilder customizeHttpAsyncClient(
        Consumer<HttpAsyncClientBuilder> asyncClientBuilderFunction) {
      asyncClientBuilderFunction.accept(this.asyncHttpClientBuilder);
      return this;
    }
  }
}
