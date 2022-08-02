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
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;


@Value
@Builder
@Slf4j
public class RestEmitterConfig {

  public static final int DEFAULT_CONNECT_TIMEOUT_SEC = 10;
  public static final int DEFAULT_READ_TIMEOUT_SEC = 10;
  public static final String DEFAULT_AUTH_TOKEN = null;
  public static final String CLIENT_VERSION_PROPERTY = "clientVersion";

  @Builder.Default
  private final String server = "http://localhost:8080";

  private final Integer timeoutSec;
  @Builder.Default
  private final boolean disableSslVerification = false;
  
  @Builder.Default
  private final String token = DEFAULT_AUTH_TOKEN;

  @Builder.Default
  @NonNull
  private final Map<String, String> extraHeaders = Collections.EMPTY_MAP;

  private final HttpAsyncClientBuilder asyncHttpClientBuilder;

  @Builder.Default
  private final EventFormatter eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);

  public static class RestEmitterConfigBuilder {

    private String getVersion() {
      try (
        InputStream foo = this.getClass().getClassLoader().getResourceAsStream("client.properties")) {
          Properties properties = new Properties();
          properties.load(foo);
          return properties.getProperty(CLIENT_VERSION_PROPERTY, "unknown");
      } catch (Exception e) {
        log.warn("Unable to find a version for datahub-client. Will set to unknown", e);
        return "unknown";
      }
    }

    private HttpAsyncClientBuilder asyncHttpClientBuilder = HttpAsyncClientBuilder
        .create()
        .setDefaultRequestConfig(RequestConfig.custom()
        .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SEC * 1000)
        .setSocketTimeout(DEFAULT_READ_TIMEOUT_SEC * 1000)
        .build())
        .setUserAgent("DataHub-RestClient/" + getVersion());

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