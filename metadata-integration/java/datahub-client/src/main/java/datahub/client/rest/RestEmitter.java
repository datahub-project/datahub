package datahub.client.rest;

import static com.linkedin.metadata.Constants.*;
import static org.apache.hc.core5.http.HttpHeaders.*;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.JacksonDataTemplateCodec;
import com.linkedin.mxe.MetadataChangeProposal;
import datahub.client.Callback;
import datahub.client.Emitter;
import datahub.client.MetadataResponseFuture;
import datahub.client.MetadataWriteResponse;
import datahub.event.EventFormatter;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.event.UpsertAspectRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.client5.http.async.methods.SimpleHttpResponse;
import org.apache.hc.client5.http.async.methods.SimpleRequestBuilder;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.config.TlsConfig;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.http2.HttpVersionPolicy;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.TimeValue;

@ThreadSafe
@Slf4j
/**
 * The REST emitter is a thin wrapper on top of the Apache HttpClient
 * (https://hc.apache.org/httpcomponents-client-4.5.x/index.html) library. It supports non-blocking
 * emission of metadata and handles the details of JSON serialization of metadata aspects over the
 * wire.
 *
 * <p>Constructing a REST Emitter follows a lambda-based fluent builder pattern using the `create`
 * method. e.g. RestEmitter emitter = RestEmitter.create(b :: b .server("http://localhost:8080")
 * .extraHeaders(Collections.singletonMap("Custom-Header", "custom-val") ); You can also customize
 * the underlying http client by calling the `customizeHttpAsyncClient` method on the builder. e.g.
 * RestEmitter emitter = RestEmitter.create(b :: b .server("http://localhost:8080")
 * .extraHeaders(Collections.singletonMap("Custom-Header", "custom-val") .customizeHttpAsyncClient(c
 * :: c.setConnectionTimeToLive(30, TimeUnit.SECONDS)) );
 */
public class RestEmitter implements Emitter {

  private final RestEmitterConfig config;
  private final String ingestProposalUrl;
  private final String ingestOpenApiUrl;
  private final String configUrl;

  private final ObjectMapper objectMapper;
  private final JacksonDataTemplateCodec dataTemplateCodec;
  private final CloseableHttpAsyncClient httpClient;
  private final EventFormatter eventFormatter;

  /**
   * The default constructor, prefer using the `create` factory method.
   *
   * @param config
   */
  public RestEmitter(RestEmitterConfig config) {
    objectMapper = new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);
    int maxSize =
        Integer.parseInt(
            System.getenv()
                .getOrDefault(INGESTION_MAX_SERIALIZED_STRING_LENGTH, MAX_JACKSON_STRING_SIZE));
    objectMapper
        .getFactory()
        .setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(maxSize).build());
    dataTemplateCodec = new JacksonDataTemplateCodec(objectMapper.getFactory());

    this.config = config;
    HttpAsyncClientBuilder httpClientBuilder = this.config.getAsyncHttpClientBuilder();
    httpClientBuilder.setRetryStrategy(new DatahubHttpRequestRetryStrategy());
    if ((config.getTimeoutSec() != null) || (config.isDisableChunkedEncoding())) {
      RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
      // Override httpClient settings with RestEmitter configs if present
      if (config.getTimeoutSec() != null) {
        requestConfigBuilder
            .setConnectionRequestTimeout(config.getTimeoutSec() * 1000, TimeUnit.MILLISECONDS)
            .setResponseTimeout(config.getTimeoutSec() * 1000, TimeUnit.MILLISECONDS);
      }
      if (config.isDisableChunkedEncoding()) {
        requestConfigBuilder.setContentCompressionEnabled(false);
      }
      httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());
    }

    PoolingAsyncClientConnectionManagerBuilder poolingAsyncClientConnectionManagerBuilder =
        PoolingAsyncClientConnectionManagerBuilder.create();

    // Forcing http 1.x as 2.0 is not supported yet
    TlsConfig tlsHttp1Config =
        TlsConfig.copy(TlsConfig.DEFAULT).setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1).build();
    poolingAsyncClientConnectionManagerBuilder.setDefaultTlsConfig(tlsHttp1Config);

    if (config.isDisableSslVerification()) {
      try {
        SSLContext sslcontext =
            SSLContexts.custom().loadTrustMaterial(TrustAllStrategy.INSTANCE).build();
        TlsStrategy tlsStrategy =
            ClientTlsStrategyBuilder.create()
                .setSslContext(sslcontext)
                .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();
        poolingAsyncClientConnectionManagerBuilder.setTlsStrategy(tlsStrategy);
      } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
        throw new RuntimeException("Error while creating insecure http client", e);
      }
    }
    httpClientBuilder.setConnectionManager(poolingAsyncClientConnectionManagerBuilder.build());

    httpClientBuilder.setRetryStrategy(
        new DatahubHttpRequestRetryStrategy(
            config.getMaxRetries(), TimeValue.ofSeconds(config.getRetryIntervalSec())));

    this.httpClient = httpClientBuilder.build();
    this.httpClient.start();
    this.ingestProposalUrl = this.config.getServer() + "/aspects?action=ingestProposal";
    this.ingestOpenApiUrl = config.getServer() + "/openapi/entities/v1/";
    this.configUrl = this.config.getServer() + "/config";
    this.eventFormatter = this.config.getEventFormatter();
  }

  private static MetadataWriteResponse mapResponse(SimpleHttpResponse response) {
    MetadataWriteResponse.MetadataWriteResponseBuilder builder =
        MetadataWriteResponse.builder().underlyingResponse(response);
    if ((response != null) && (response.getCode()) == HttpStatus.SC_OK
        || Objects.requireNonNull(response).getCode() == HttpStatus.SC_CREATED) {
      builder.success(true);
    } else {
      builder.success(false);
    }
    // Read response content
    try {
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      builder.responseContent(response.getBody().getBodyText());
    } catch (Exception e) {
      // Catch all exceptions and still return a valid response object
      log.warn("Wasn't able to convert response into a string", e);
    }
    return builder.build();
  }

  /**
   * Constructing a REST Emitter follows a lambda-based fluent builder pattern using the `create`
   * method. e.g. RestEmitter emitter = RestEmitter.create(b :: b .server("http://localhost:8080")
   * // coordinates of gms server .extraHeaders(Collections.singletonMap("Custom-Header",
   * "custom-val") ); You can also customize the underlying http client by calling the
   * `customizeHttpAsyncClient` method on the builder. e.g. RestEmitter emitter =
   * RestEmitter.create(b :: b .server("http://localhost:8080")
   * .extraHeaders(Collections.singletonMap("Custom-Header", "custom-val")
   * .customizeHttpAsyncClient(c :: c.setConnectionTimeToLive(30, TimeUnit.SECONDS)) );
   *
   * @param builderSupplier
   * @return a constructed RestEmitter. Call #testConnection to make sure this emitter has a valid
   *     connection to the server
   */
  public static RestEmitter create(
      Consumer<RestEmitterConfig.RestEmitterConfigBuilder> builderSupplier) {
    RestEmitter restEmitter =
        new RestEmitter(RestEmitterConfig.builder().with(builderSupplier).build());
    return restEmitter;
  }

  /**
   * Creates a RestEmitter with default settings.
   *
   * @return a constructed RestEmitter. Call #test_connection to validate that this emitter can
   *     communicate with the server.
   */
  public static RestEmitter createWithDefaults() {
    // No-op creator -> creates RestEmitter using default settings
    return create(b -> {});
  }

  @Override
  public Future<MetadataWriteResponse> emit(MetadataChangeProposalWrapper mcpw, Callback callback)
      throws IOException {
    return emit(this.eventFormatter.convert(mcpw), callback);
  }

  @Override
  public Future<MetadataWriteResponse> emit(MetadataChangeProposal mcp, Callback callback)
      throws IOException {
    DataMap map = new DataMap();
    map.put("proposal", mcp.data());
    String serializedMCP = dataTemplateCodec.mapToString(map);
    log.debug("Emit: URL: {}, Payload: {}\n", this.ingestProposalUrl, serializedMCP);
    return this.postGeneric(this.ingestProposalUrl, serializedMCP, mcp, callback);
  }

  private Future<MetadataWriteResponse> postGeneric(
      String urlStr, String payloadJson, Object originalRequest, Callback callback)
      throws IOException {
    SimpleRequestBuilder simpleRequestBuilder = SimpleRequestBuilder.post(urlStr);
    simpleRequestBuilder.setHeader("Content-Type", "application/json");
    simpleRequestBuilder.setHeader("X-RestLi-Protocol-Version", "2.0.0");
    simpleRequestBuilder.setHeader("Accept", "application/json");
    this.config.getExtraHeaders().forEach(simpleRequestBuilder::setHeader);
    if (this.config.getToken() != null) {
      simpleRequestBuilder.setHeader("Authorization", "Bearer " + this.config.getToken());
    }
    if (this.config.isDisableChunkedEncoding()) {
      byte[] payloadBytes = payloadJson.getBytes(StandardCharsets.UTF_8);
      simpleRequestBuilder.setBody(payloadBytes, ContentType.APPLICATION_JSON);
    } else {
      simpleRequestBuilder.setBody(payloadJson, ContentType.APPLICATION_JSON);
    }

    AtomicReference<MetadataWriteResponse> responseAtomicReference = new AtomicReference<>();
    CountDownLatch responseLatch = new CountDownLatch(1);
    FutureCallback<SimpleHttpResponse> httpCallback =
        new FutureCallback<SimpleHttpResponse>() {
          @Override
          public void completed(SimpleHttpResponse response) {
            MetadataWriteResponse writeResponse = null;
            try {
              writeResponse = mapResponse(response);
              responseAtomicReference.set(writeResponse);
            } catch (Exception e) {
              // do nothing
            }
            responseLatch.countDown();
            if (callback != null) {
              try {
                callback.onCompletion(writeResponse);
              } catch (Exception e) {
                log.error("Error executing user callback on completion.", e);
              }
            }
          }

          @Override
          public void failed(Exception ex) {
            responseLatch.countDown();
            if (callback != null) {
              try {
                callback.onFailure(ex);
              } catch (Exception e) {
                log.error("Error executing user callback on failure.", e);
              }
            }
          }

          @Override
          public void cancelled() {
            responseLatch.countDown();
            if (callback != null) {
              try {
                callback.onFailure(new RuntimeException("Cancelled"));
              } catch (Exception e) {
                log.error("Error executing user callback on failure due to cancellation.", e);
              }
            }
          }
        };
    Future<SimpleHttpResponse> requestFuture =
        httpClient.execute(simpleRequestBuilder.build(), httpCallback);
    return new MetadataResponseFuture(requestFuture, responseAtomicReference, responseLatch);
  }

  private Future<MetadataWriteResponse> getGeneric(String urlStr) throws IOException {
    SimpleHttpRequest simpleHttpRequest =
        SimpleRequestBuilder.get(urlStr)
            .addHeader("Content-Type", "application/json")
            .addHeader("X-RestLi-Protocol-Version", "2.0.0")
            .addHeader("Accept", "application/json")
            .build();

    Future<SimpleHttpResponse> response = this.httpClient.execute(simpleHttpRequest, null);
    return new MetadataResponseFuture(response, RestEmitter::mapResponse);
  }

  @Override
  public boolean testConnection() throws IOException, ExecutionException, InterruptedException {
    return this.getGeneric(this.configUrl).get().isSuccess();
  }

  @Override
  public void close() throws IOException {
    this.httpClient.close();
  }

  @Override
  public Future<MetadataWriteResponse> emit(List<UpsertAspectRequest> request, Callback callback)
      throws IOException {
    log.debug("Emit: URL: {}, Payload: {}\n", this.ingestOpenApiUrl, request);
    return this.postOpenAPI(request, callback);
  }

  private Future<MetadataWriteResponse> postOpenAPI(
      List<UpsertAspectRequest> payload, Callback callback) throws IOException {
    SimpleRequestBuilder simpleRequestBuilder =
        SimpleRequestBuilder.post(ingestOpenApiUrl)
            .addHeader("Content-Type", "application/json")
            .addHeader("Accept", "application/json")
            .addHeader("X-RestLi-Protocol-Version", "2.0.0");

    this.config.getExtraHeaders().forEach(simpleRequestBuilder::addHeader);

    if (this.config.getToken() != null) {
      simpleRequestBuilder.addHeader("Authorization", "Bearer " + this.config.getToken());
    }
    simpleRequestBuilder.setBody(
        objectMapper.writeValueAsString(payload), ContentType.APPLICATION_JSON);
    AtomicReference<MetadataWriteResponse> responseAtomicReference = new AtomicReference<>();
    CountDownLatch responseLatch = new CountDownLatch(1);
    FutureCallback<SimpleHttpResponse> httpCallback =
        new FutureCallback<SimpleHttpResponse>() {
          @Override
          public void completed(SimpleHttpResponse response) {
            MetadataWriteResponse writeResponse = null;
            try {
              writeResponse = mapResponse(response);
              responseAtomicReference.set(writeResponse);
            } catch (Exception e) {
              // do nothing
            }
            responseLatch.countDown();
            if (callback != null) {
              try {
                callback.onCompletion(writeResponse);
              } catch (Exception e) {
                log.error("Error executing user callback on completion.", e);
              }
            }
          }

          @Override
          public void failed(Exception ex) {
            responseLatch.countDown();
            if (callback != null) {
              try {
                callback.onFailure(ex);
              } catch (Exception e) {
                log.error("Error executing user callback on failure.", e);
              }
            }
          }

          @Override
          public void cancelled() {
            responseLatch.countDown();
            if (callback != null) {
              try {
                callback.onFailure(new RuntimeException("Cancelled"));
              } catch (Exception e) {
                log.error("Error executing user callback on failure due to cancellation.", e);
              }
            }
          }
        };
    Future<SimpleHttpResponse> requestFuture =
        httpClient.execute(simpleRequestBuilder.build(), httpCallback);
    return new MetadataResponseFuture(requestFuture, responseAtomicReference, responseLatch);
  }

  @VisibleForTesting
  CloseableHttpAsyncClient getHttpClient() {
    return this.httpClient;
  }
}
