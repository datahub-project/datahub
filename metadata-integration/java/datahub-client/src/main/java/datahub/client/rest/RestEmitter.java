package datahub.client.rest;

import static com.linkedin.metadata.Constants.*;

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
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.TrustAllStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.nio.client.HttpAsyncClient;
import org.apache.http.ssl.SSLContextBuilder;

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
    // Override httpClient settings with RestEmitter configs if present
    if (config.getTimeoutSec() != null) {
      HttpAsyncClientBuilder httpClientBuilder = this.config.getAsyncHttpClientBuilder();
      httpClientBuilder.setDefaultRequestConfig(
          RequestConfig.custom()
              .setConnectTimeout(config.getTimeoutSec() * 1000)
              .setSocketTimeout(config.getTimeoutSec() * 1000)
              .build());
    }
    if (config.isDisableSslVerification()) {
      HttpAsyncClientBuilder httpClientBuilder = this.config.getAsyncHttpClientBuilder();
      try {
        httpClientBuilder
            .setSSLContext(
                new SSLContextBuilder().loadTrustMaterial(null, TrustAllStrategy.INSTANCE).build())
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
      } catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
        throw new RuntimeException("Error while creating insecure http client", e);
      }
    }

    this.httpClient = this.config.getAsyncHttpClientBuilder().build();
    this.httpClient.start();
    this.ingestProposalUrl = this.config.getServer() + "/aspects?action=ingestProposal";
    this.ingestOpenApiUrl = config.getServer() + "/openapi/entities/v1/";
    this.configUrl = this.config.getServer() + "/config";
    this.eventFormatter = this.config.getEventFormatter();
  }

  private static MetadataWriteResponse mapResponse(HttpResponse response) {
    MetadataWriteResponse.MetadataWriteResponseBuilder builder =
        MetadataWriteResponse.builder().underlyingResponse(response);
    if ((response != null)
        && (response.getStatusLine() != null)
        && (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK
            || response.getStatusLine().getStatusCode() == HttpStatus.SC_CREATED)) {
      builder.success(true);
    } else {
      builder.success(false);
    }
    // Read response content
    try {
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      InputStream contentStream = response.getEntity().getContent();
      byte[] buffer = new byte[1024];
      int length = contentStream.read(buffer);
      while (length > 0) {
        result.write(buffer, 0, length);
        length = contentStream.read(buffer);
      }
      builder.responseContent(result.toString("UTF-8"));
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
    HttpPost httpPost = new HttpPost(urlStr);
    httpPost.setHeader("Content-Type", "application/json");
    httpPost.setHeader("X-RestLi-Protocol-Version", "2.0.0");
    httpPost.setHeader("Accept", "application/json");
    this.config.getExtraHeaders().forEach((k, v) -> httpPost.setHeader(k, v));
    if (this.config.getToken() != null) {
      httpPost.setHeader("Authorization", "Bearer " + this.config.getToken());
    }
    httpPost.setEntity(new StringEntity(payloadJson));
    AtomicReference<MetadataWriteResponse> responseAtomicReference = new AtomicReference<>();
    CountDownLatch responseLatch = new CountDownLatch(1);
    FutureCallback<HttpResponse> httpCallback =
        new FutureCallback<HttpResponse>() {
          @Override
          public void completed(HttpResponse response) {
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
            if (callback != null) {
              try {
                callback.onFailure(new RuntimeException("Cancelled"));
              } catch (Exception e) {
                log.error("Error executing user callback on failure due to cancellation.", e);
              }
            }
          }
        };
    Future<HttpResponse> requestFuture = httpClient.execute(httpPost, httpCallback);
    return new MetadataResponseFuture(requestFuture, responseAtomicReference, responseLatch);
  }

  private Future<MetadataWriteResponse> getGeneric(String urlStr) throws IOException {
    HttpGet httpGet = new HttpGet(urlStr);
    httpGet.setHeader("Content-Type", "application/json");
    httpGet.setHeader("X-RestLi-Protocol-Version", "2.0.0");
    httpGet.setHeader("Accept", "application/json");
    Future<HttpResponse> response = this.httpClient.execute(httpGet, null);
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
    HttpPost httpPost = new HttpPost(ingestOpenApiUrl);
    httpPost.setHeader("Content-Type", "application/json");
    httpPost.setHeader("Accept", "application/json");
    this.config.getExtraHeaders().forEach((k, v) -> httpPost.setHeader(k, v));
    if (this.config.getToken() != null) {
      httpPost.setHeader("Authorization", "Bearer " + this.config.getToken());
    }
    httpPost.setEntity(new StringEntity(objectMapper.writeValueAsString(payload)));
    AtomicReference<MetadataWriteResponse> responseAtomicReference = new AtomicReference<>();
    CountDownLatch responseLatch = new CountDownLatch(1);
    FutureCallback<HttpResponse> httpCallback =
        new FutureCallback<HttpResponse>() {
          @Override
          public void completed(HttpResponse response) {
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
            if (callback != null) {
              try {
                callback.onFailure(new RuntimeException("Cancelled"));
              } catch (Exception e) {
                log.error("Error executing user callback on failure due to cancellation.", e);
              }
            }
          }
        };
    Future<HttpResponse> requestFuture = httpClient.execute(httpPost, httpCallback);
    return new MetadataResponseFuture(requestFuture, responseAtomicReference, responseLatch);
  }

  @VisibleForTesting
  HttpAsyncClient getHttpClient() {
    return this.httpClient;
  }
}
