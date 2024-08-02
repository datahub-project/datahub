package com.linkedin.metadata.integration;

import com.datahub.authentication.Authentication;
import com.datahub.util.RecordUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.event.notification.NotificationRequest;
import com.linkedin.link.LinkPreviewInfo;
import com.linkedin.link.LinkPreviewType;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.parseq.retry.backoff.BackoffPolicy;
import com.linkedin.parseq.retry.backoff.ExponentialBackoff;
import io.datahubproject.integrations.api.ActionsApi;
import io.datahubproject.integrations.api.AiApi;
import io.datahubproject.integrations.api.AnalyticsApi;
import io.datahubproject.integrations.api.ShareApi;
import io.datahubproject.integrations.invoker.ApiClient;
import io.datahubproject.integrations.invoker.ApiException;
import io.datahubproject.integrations.invoker.ApiResponse;
import io.datahubproject.integrations.invoker.ServerConfiguration;
import io.datahubproject.integrations.model.ExecuteShareResult;
import io.datahubproject.integrations.model.ExecuteUnshareResult;
import io.datahubproject.integrations.model.LineageDirection;
import io.datahubproject.integrations.model.SuggestedDescription;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/** This class is responsible for coordinating authentication with the backend Metadata Service. */
@Slf4j
public class IntegrationsService {

  private static final int DEFAULT_RETRY_INTERVAL = 2;

  private static final String RELOAD_CREDENTIALS_ENDPOINT = "private/reload_credentials";
  private static final String GET_LINK_PREVIEW_ENDPOINT = "private/get_link_preview";
  private static final String SLACK_MESSAGE_URL_PATTERN = ".*.slack.com/archives/.*";

  private static final String REGISTER_ACTION_ENDPOINT = "private/actions/register";

  private static final String SEND_NOTIFICATION_ENDPOINT = "private/notifications/send";
  private static final String ANALYTICS_QUERY_ENDPOINT = "private/analytics/query";

  private final String integrationsServiceHost;
  private final Integer integrationsServicePort;
  private final Authentication systemAuthentication;
  private final CloseableHttpClient httpClient;
  private final String protocol;
  private final BackoffPolicy backoffPolicy;
  private final int retryCount;
  private final int timeoutSeconds;

  private final ActionsApi actionsApi;
  private final AiApi aiApi;
  @Getter private final AnalyticsApi analyticsApi;
  private final ShareApi shareApi;
  private final ObjectMapper objectMapper = new ObjectMapper();

  public IntegrationsService(
      @Nonnull final String integrationsServiceHost,
      @Nonnull final Integer integrationsServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final Authentication systemAuthentication) {
    this(
        integrationsServiceHost,
        integrationsServicePort,
        useSsl,
        systemAuthentication,
        HttpClients.createDefault(),
        new ExponentialBackoff(DEFAULT_RETRY_INTERVAL),
        3,
        30);
  }

  public IntegrationsService(
      @Nonnull final String integrationsServiceHost,
      @Nonnull final Integer integrationsServicePort,
      @Nonnull final Boolean useSsl,
      @Nonnull final Authentication systemAuthentication,
      @Nonnull final CloseableHttpClient httpClient,
      @Nonnull final BackoffPolicy backoffPolicy,
      final int retryCount,
      @Nonnull final Integer timeoutSeconds) {
    this.integrationsServiceHost = Objects.requireNonNull(integrationsServiceHost);
    this.integrationsServicePort = Objects.requireNonNull(integrationsServicePort);
    this.systemAuthentication = Objects.requireNonNull(systemAuthentication);
    this.httpClient = Objects.requireNonNull(httpClient);
    this.protocol = useSsl ? "https" : "http";
    this.backoffPolicy = backoffPolicy;
    this.retryCount = retryCount;
    this.timeoutSeconds = Objects.requireNonNull(timeoutSeconds);
    ApiClient okHttpClient = new ApiClient(); // TODO: configure retries, backoff, etc.
    okHttpClient.setServers(
        ImmutableList.of(
            new ServerConfiguration(
                String.format(
                    "%s://%s:%d",
                    this.protocol, this.integrationsServiceHost, this.integrationsServicePort),
                "",
                Collections.EMPTY_MAP)));
    okHttpClient.setReadTimeout(timeoutSeconds * 1000);
    this.actionsApi = new ActionsApi(okHttpClient);
    this.aiApi = new AiApi(okHttpClient);
    this.analyticsApi = new AnalyticsApi(okHttpClient);
    this.shareApi = new ShareApi(okHttpClient);
  }

  /** Calls the integration service to refresh their connection settings on demand. */
  public void reloadCredentials() {
    CloseableHttpResponse response = null;
    try {
      // Build request
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s/%s",
                  protocol,
                  this.integrationsServiceHost,
                  this.integrationsServicePort,
                  RELOAD_CREDENTIALS_ENDPOINT));

      addRequestHeaders(request);

      // Execute request
      response = executeRequest(request);

      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        log.error(
            "Failed to refresh integration settings after retrying! Integrations service returned non-200 error code!");
      }
    } catch (Exception e) {
      log.error(
          "Failed to refresh integration settings after retrying! Exceptions encountered when trying to access integrations service");
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response to integration service.", e);
      }
    }
  }

  /**
   * Attempt to resolve a Link Preview for a given URL. This method returns null if a link preview
   * fails to resolve.
   */
  @Nullable
  public LinkPreviewInfo getLinkPreview(@Nonnull final String url) {
    Objects.requireNonNull(url, "url must not be null");

    // First - determine the preview type we should request.
    LinkPreviewType type = getLinkPreviewType(url);
    if (type == null) {
      // Cannot resolve a preview.
      return null;
    }

    CloseableHttpResponse response = null;
    try {

      // Build request
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s/%s",
                  protocol,
                  this.integrationsServiceHost,
                  this.integrationsServicePort,
                  GET_LINK_PREVIEW_ENDPOINT));

      addRequestHeaders(request);

      final String jsonBody = buildGetLinkPreviewBodyJson(type, url);
      request.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

      // Execute request
      response = executeRequest(request);
      final HttpEntity entity = response.getEntity();

      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK && entity != null) {
        final String jsonStr = EntityUtils.toString(entity);
        return buildGetLinkPreviewResult(type, jsonStr);
      }
      // Otherwise, something went wrong!
      log.error(
          String.format(
              "Bad response from the Integrations Service: %s",
              response.getStatusLine().toString()));
      return null;
    } catch (Exception e) {
      log.error("Failed to retrieve link preview notification.", e);
      return null;
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response to integration service.", e);
      }
    }
  }

  public void sendNotification(@Nonnull final NotificationRequest notificationRequest) {
    Objects.requireNonNull(notificationRequest, "notificationRequest must not be null");

    CloseableHttpResponse response = null;
    try {

      // Build request
      final HttpPost request =
          new HttpPost(
              String.format(
                  "%s://%s:%s/%s",
                  protocol,
                  this.integrationsServiceHost,
                  this.integrationsServicePort,
                  SEND_NOTIFICATION_ENDPOINT));

      addRequestHeaders(request);

      final String jsonBody = buildNotificationRequestBodyJson(notificationRequest);
      request.setEntity(new StringEntity(jsonBody, StandardCharsets.UTF_8));

      // Execute request
      response = executeRequest(request);

      if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        log.debug("Successfully sent notification to integrations service.");
      } else {
        // Otherwise, something went wrong. We bubble this up.
        throw new RuntimeException(
            String.format(
                "Failed to send notification! Bad response from the Integrations Service: %s",
                response.getStatusLine().toString()));
      }
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to send notification! Caught exception while attempting to send notification to integrations ervice.",
          e);
    } finally {
      try {
        if (response != null) {
          response.close();
        }
      } catch (Exception e) {
        log.error("Failed to close http response to integration service.", e);
      }
    }
  }

  private void addRequestHeaders(@Nonnull final HttpUriRequest request) {
    // Add authorization header with DataHub frontend system id and secret.
    request.addHeader("Authorization", this.systemAuthentication.getCredentials());
    request.addHeader("Content-Type", "application/json");
  }

  private CloseableHttpResponse executeRequest(@Nonnull final HttpUriRequest request)
      throws Exception {
    int attemptCount = 0;
    while (attemptCount < this.retryCount) {
      try {
        return httpClient.execute(request);
      } catch (Exception ex) {
        MetricUtils.counter(
                IntegrationsService.class,
                "exception" + MetricUtils.DELIMITER + ex.getClass().getName().toLowerCase())
            .inc();
        if (attemptCount == this.retryCount - 1) {
          throw ex;
        } else {
          attemptCount = attemptCount + 1;
          Thread.sleep(this.backoffPolicy.nextBackoff(attemptCount, ex) * 1000);
        }
      }
    }
    // Should never hit this line.
    throw new RuntimeException("Failed to execute request to integrations service!");
  }

  private static String buildGetLinkPreviewBodyJson(
      @Nonnull final LinkPreviewType type, @Nonnull final String url) throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    objectNode.put("type", type.toString());
    objectNode.put("url", url);
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  private static String buildNotificationRequestBodyJson(
      @Nonnull final NotificationRequest notificationRequest) {
    return RecordUtils.toJsonString(notificationRequest);
  }

  private static LinkPreviewInfo buildGetLinkPreviewResult(
      @Nonnull final LinkPreviewType type, @Nonnull final String jsonStr) {

    ObjectMapper mapper = new ObjectMapper();
    try {
      ObjectNode json = (ObjectNode) mapper.readTree(jsonStr);
      final LinkPreviewInfo result = new LinkPreviewInfo();
      // Integrations service MUST provide a valid preview type.
      result.setType(type);
      Long lastRefreshedMs = System.currentTimeMillis();
      if (json.has("lastRefreshedMs")) {
        lastRefreshedMs = json.get("lastRefreshedMs").asLong();
      }
      result.setLastRefreshedMs(lastRefreshedMs);
      // Write the encoded json.
      result.setJson(json.toString());
      return result;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse JSON received from the Integrations service!");
    }
  }

  /**
   * Determines what link Preview Type should be requested based on the link URL.
   *
   * <p>In the future, we'll likely push this inference down into the Integration Service itself.
   *
   * @param url the url to fetch the preview for.
   * @return the {@link LinkPreviewType} that should be retrieved for the given URL, or null if a
   *     matching Link Preview Type cannot be found.
   */
  @Nullable
  private LinkPreviewType getLinkPreviewType(@Nonnull final String url) {
    Pattern pattern = Pattern.compile(SLACK_MESSAGE_URL_PATTERN);
    if (pattern.matcher(url).matches()) {
      return LinkPreviewType.SLACK_MESSAGE;
    } else {
      log.warn(
          String.format(
              "Received request to provide link preview for unsupported URL %s. Skipping link preview",
              url));
      return null;
    }
  }

  private static String buildRegisterActionBodyJson(
      @Nonnull final Urn actionUrn, @Nonnull final String recipe) throws Exception {
    final ObjectMapper objectMapper = new ObjectMapper();
    final ObjectNode objectNode = objectMapper.createObjectNode();
    objectNode.put("action_urn", actionUrn.toString());
    objectNode.put("action_config", recipe);
    return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(objectNode);
  }

  public boolean reloadAction(String actionPipelineUrn) {

    ApiResponse<Object> response = null;
    try {
      response = this.actionsApi.reloadActionWithHttpInfo(actionPipelineUrn);

      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error("Failed to reload action! Integrations service returned non-200 error code!");
        log.error(String.valueOf(response.getData().toString()));
        return false;
      }
      return true;
    } catch (Exception e) {
      log.error(
          "Failed to reload action! Exceptions encountered when trying to access integrations service");
      return false;
    }
  }

  public SuggestedDescription suggestDescription(Urn entity) {
    try {
      var response = this.aiApi.suggestDescriptionWithHttpInfo(entity.toString());
      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error(
            "Failed to suggest description for entity! Integrations service returned non-200 error code!");
        log.error(String.valueOf(response.getData().toString()));
        return null;
      }

      return response.getData();
    } catch (ApiException e) {
      log.error("Failed to suggest description for entity: " + entity.toString(), e);
      return null;
    }
  }

  public boolean query(
      String entityUrn,
      String query_string,
      Consumer<List<String>> headerProcessor,
      Consumer<List<String>> rowProcessor,
      Consumer<List<String>> errorProcessor) {
    try {
      // Encode the query parameters to handle special characters
      String encodedEntityUrn = URLEncoder.encode(entityUrn, StandardCharsets.UTF_8);
      String encodedQueryFragment = URLEncoder.encode(query_string, StandardCharsets.UTF_8);

      String requestURI =
          String.format(
              "%s://%s:%s/%s?entity_urn=%s&format=tuple&sql_query_fragment=%s",
              protocol,
              this.integrationsServiceHost,
              this.integrationsServicePort,
              ANALYTICS_QUERY_ENDPOINT,
              encodedEntityUrn,
              encodedQueryFragment);
      StreamingHttpClient streamingHttpClient = new StreamingHttpClient();
      streamingHttpClient.queryAndProcessStream(
          requestURI,
          Map.of(
              "Content-Type",
              "application/json",
              "Authorization",
              this.systemAuthentication.getCredentials()),
          headerProcessor,
          rowProcessor,
          errorProcessor);
      return true;
    } catch (ResourceNotFoundException e) {
      log.error("Failed to query analytics service due to resource not found", e);
      throw e;
    } catch (CompletionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof java.net.ConnectException) {
        // Handle connection failures specifically
        // Log the error or return a user-friendly message
        log.error("Connection failed: " + cause.getMessage());
        throw new RuntimeException("Connection failed: " + cause.getMessage(), cause);
      } else {
        // Handle other unexpected exceptions
        log.error("Unexpected error: " + cause.getMessage());
        throw new RuntimeException("Unexpected error: " + cause.getMessage(), cause);
      }
    } catch (Exception e) {
      // Handle other non-CompletionException errors
      log.error("An error occurred: " + e.getMessage());
      throw new RuntimeException("An error occurred: " + e.getMessage(), e);
    }
  }

  public ExecuteShareResult shareEntity(
      @Nonnull final Urn connectionUrn,
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn sharerUrn,
      LineageDirection lineageDirection) {
    try {
      // LineageDirection is always null now because we are not using it yet.
      ApiResponse<ExecuteShareResult> response =
          this.shareApi.executeShareWithHttpInfo(
              connectionUrn.toString(),
              entityUrn.toString(),
              sharerUrn.toString(),
              lineageDirection);
      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error(
            String.format(
                "Failed to share entity with urn %s. Integrations service returned non-200 error code.",
                entityUrn));
        log.error(String.valueOf(response.getData().toString()));
        return null;
      }
      return response.getData();
    } catch (ApiException e) {
      log.error("Failed to share entity with urn: " + entityUrn, e);
      return null;
    }
  }

  public ExecuteUnshareResult unshareEntity(
      @Nonnull final Urn connectionUrn,
      @Nonnull final Urn entityUrn,
      LineageDirection lineageDirection) {
    try {
      // LineageDirection is always null now because we are not using it yet.
      ApiResponse<ExecuteUnshareResult> response =
          this.shareApi.executeUnshareWithHttpInfo(
              connectionUrn.toString(), entityUrn.toString(), lineageDirection);
      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error(
            String.format(
                "Failed to unshare entity with urn %s. Integrations service returned non-200 error code.",
                entityUrn));
        log.error(String.valueOf(response.getData().toString()));
        return null;
      }

      return response.getData();
    } catch (ApiException e) {
      log.error("Failed to unshare entity with urn: " + entityUrn, e);
      return null;
    }
  }

  public boolean rollbackAction(String actionPipelineUrn) {

    ApiResponse<Object> response = null;
    try {
      Object apiResponse = this.actionsApi.rollbackActionWithHttpInfo(actionPipelineUrn);
      response = (ApiResponse<Object>) apiResponse;

      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error("Failed to rollback action! Integrations service returned non-200 error code!");
        log.error(String.valueOf(response.getData().toString()));
        return false;
      }
      return true;
    } catch (Exception e) {
      log.error(
          "Failed to rollback action! Exceptions encountered when trying to access integrations service",
          e);
      return false;
    }
  }

  public boolean bootstrapAction(String actionPipelineUrn) {

    ApiResponse<Object> response = null;
    try {
      Object apiResponse = this.actionsApi.bootstrapActionWithHttpInfo(actionPipelineUrn);
      response = (ApiResponse<Object>) apiResponse;

      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error("Failed to bootstrap action! Integrations service returned non-200 error code!");
        log.error(String.valueOf(response.getData().toString()));
        return false;
      }
      return true;
    } catch (Exception e) {
      log.error(
          "Failed to bootstrap action! Exceptions encountered when trying to access integrations service",
          e);
      return false;
    }
  }

  public boolean stopAction(String actionPipelineUrn) {

    log.info("Stopping action pipeline = {}", actionPipelineUrn);
    ApiResponse<Object> response = null;
    try {
      Object apiResponse = this.actionsApi.stopActionWithHttpInfo(actionPipelineUrn);
      response = (ApiResponse<Object>) apiResponse;
      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error("Failed to stop action! Integrations service returned non-200 error code!");
        log.error(String.valueOf(response.getData().toString()));
        return false;
      }
      return true;
    } catch (Exception e) {
      log.error(
          "Failed to stop action! Exceptions encountered when trying to access integrations service",
          e);
      return false;
    }
  }

  public String actionStatus(String actionPipelineUrn) {
    ApiResponse<Object> response = null;
    try {
      Object apiResponse = this.actionsApi.actionStatsWithHttpInfo(actionPipelineUrn);
      response = (ApiResponse<Object>) apiResponse;
      if (response.getStatusCode() != HttpStatus.SC_OK) {
        log.error("Failed to get action status! Integrations service returned non-200 error code!");
        log.error(String.valueOf(response.getData().toString()));
        return null;
      }
      Object apiResponseData = response.getData();

      return this.objectMapper.writeValueAsString(response.getData());
    } catch (Exception e) {
      log.error(
          "Failed to get action status! Exceptions encountered when trying to access integrations service",
          e);
      return null;
    }
  }
}
