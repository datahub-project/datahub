package datahub.client.rest;

import datahub.event.EventFormatter;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;


@Value
@Builder
public class RestEmitterConfig {

  public static final int DEFAULT_CONNECT_TIMEOUT_SEC = 10;
  public static final int DEFAULT_READ_TIMEOUT_SEC = 10;
  public static final String DEFAULT_AUTH_TOKEN = null;

  @Builder.Default
  private final String server = "http://localhost:8080";

  private final Integer timeoutSec;

  @Builder.Default
  private final String token = DEFAULT_AUTH_TOKEN;

  @Builder.Default
  @NonNull
  private final Map<String, String> extraHeaders = Collections.EMPTY_MAP;

  private final HttpAsyncClientBuilder asyncHttpClientBuilder;

  @Builder.Default
  private final EventFormatter eventFormatter = new EventFormatter(EventFormatter.Format.PEGASUS_JSON);

  public static class RestEmitterConfigBuilder {

    private HttpAsyncClientBuilder asyncHttpClientBuilder = HttpAsyncClientBuilder
        .create()
        .setDefaultRequestConfig(RequestConfig.custom()
        .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT_SEC * 1000)
        .setSocketTimeout(DEFAULT_READ_TIMEOUT_SEC * 1000)
        .build());

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