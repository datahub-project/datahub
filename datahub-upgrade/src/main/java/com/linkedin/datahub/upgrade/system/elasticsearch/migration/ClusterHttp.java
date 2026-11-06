package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;

/**
 * The migration's view of one cluster: raw JSON requests through the cluster's own shim, so every
 * call carries that cluster's configured authentication and works on OpenSearch and Elasticsearch
 * alike. Everything the copy, replay, adoption and verification need is a plain REST call.
 */
@Slf4j
public class ClusterHttp {
  private final OperationContext opContext;
  private final SearchClientShim<?> client;
  private final ObjectMapper mapper;

  public ClusterHttp(
      @Nonnull final OperationContext opContext, @Nonnull final SearchClientShim<?> client) {
    this.opContext = opContext;
    this.client = client;
    this.mapper = opContext.getObjectMapper();
  }

  @Nonnull
  public SearchClientShim<?> getClient() {
    return client;
  }

  @Nonnull
  public ObjectMapper getMapper() {
    return mapper;
  }

  @Nonnull
  public ObjectNode object() {
    return mapper.createObjectNode();
  }

  /**
   * Sends the request and parses the body. A status outside 2xx throws unless it is one of the
   * tolerated ones, whose body is returned like any other.
   */
  @Nonnull
  public JsonNode json(
      @Nonnull final String method,
      @Nonnull final String endpoint,
      @Nullable final JsonNode body,
      @Nonnull final String... toleratedStatuses)
      throws IOException {
    return answer(method, endpoint, body, toleratedStatuses).body();
  }

  /** Like {@link #json} but returns null for 404 instead of a body. */
  @Nullable
  public JsonNode jsonOrNull(
      @Nonnull final String method, @Nonnull final String endpoint, @Nullable final JsonNode body)
      throws IOException {
    final Answer answer = answer(method, endpoint, body, "404");
    return answer.status() == 404 ? null : answer.body();
  }

  /** A response the caller inspects itself: the status and the parsed body. */
  public record Answer(int status, @Nonnull JsonNode body) {}

  /**
   * Like {@link #json} but hands back the status with the body, for callers that need the cluster's
   * reason to decide what to do.
   */
  @Nonnull
  public Answer answer(
      @Nonnull final String method,
      @Nonnull final String endpoint,
      @Nullable final JsonNode body,
      @Nonnull final String... toleratedStatuses)
      throws IOException {
    return checked(send(method, endpoint, body, toleratedStatuses), toleratedStatuses);
  }

  @Nonnull
  public RawResponse send(
      @Nonnull final String method,
      @Nonnull final String endpoint,
      @Nullable final JsonNode body,
      @Nonnull final String... toleratedStatuses)
      throws IOException {
    final Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(mapper.writeValueAsString(body));
    }
    if (toleratedStatuses.length > 0) {
      request.addParameter("ignore", String.join(",", toleratedStatuses));
    }
    log.debug("{} => {}", method, endpoint);
    return client.performLowLevelRequest(opContext, request);
  }

  /** A JSON request carrying one extra header, for example the opaque id a task is found by. */
  @Nonnull
  public JsonNode jsonWithHeader(
      @Nonnull final String method,
      @Nonnull final String endpoint,
      @Nullable final JsonNode body,
      @Nonnull final String headerName,
      @Nonnull final String headerValue)
      throws IOException {
    final Request request = new Request(method, endpoint);
    if (body != null) {
      request.setJsonEntity(mapper.writeValueAsString(body));
    }
    request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader(headerName, headerValue));
    log.debug("{} => {} ({}: {})", method, endpoint, headerName, headerValue);
    return checked(client.performLowLevelRequest(opContext, request)).body();
  }

  /** Sends a pre-rendered body (for example NDJSON) with the given content type, as UTF-8. */
  @Nonnull
  public JsonNode raw(
      @Nonnull final String method,
      @Nonnull final String endpoint,
      @Nonnull final String body,
      @Nonnull final String contentType)
      throws IOException {
    final Request request = new Request(method, endpoint);
    // The charset must be explicit: a content type without one makes the entity encode the body as
    // ISO-8859-1, which the cluster rejects as invalid UTF-8 for characters up to U+00FF and which
    // turns everything beyond into '?'.
    request.setEntity(
        new NStringEntity(body, ContentType.create(contentType, StandardCharsets.UTF_8)));
    return checked(client.performLowLevelRequest(opContext, request)).body();
  }

  @Nonnull
  private Answer checked(
      @Nonnull final RawResponse response, @Nonnull final String... toleratedStatuses)
      throws IOException {
    final int status = response.getStatusLine().getStatusCode();
    // HEAD answers and some acknowledgements carry no body at all.
    final byte[] content =
        response.getEntity() == null ? new byte[0] : EntityUtils.toByteArray(response.getEntity());
    final JsonNode body =
        content.length == 0 ? mapper.createObjectNode() : mapper.readTree(content);
    if (status >= 400 && !tolerated(status, toleratedStatuses)) {
      throw new IOException(
          "Search cluster request failed with status "
              + status
              + ": "
              + body.toString().substring(0, Math.min(body.toString().length(), 2000)));
    }
    return new Answer(status, body);
  }

  private static boolean tolerated(final int status, @Nonnull final String[] toleratedStatuses) {
    final String asText = String.valueOf(status);
    for (String tolerated : toleratedStatuses) {
      if (tolerated.equals(asText)) {
        return true;
      }
    }
    return false;
  }

  // ---- common operations --------------------------------------------------------------------

  /** Whether the index exists; anything but a clear yes or no (an outage, a refusal) throws. */
  public boolean indexExists(@Nonnull final String index) throws IOException {
    return answer("HEAD", "/" + index, null, "404").status() == 200;
  }

  public void refresh(@Nonnull final String index) throws IOException {
    json("POST", "/" + index + "/_refresh", null);
  }

  /**
   * Document count under {@code filter} (a query body, or null for all), as the index stands:
   * {@linkplain #refresh refresh} first where the figure must include what was just written.
   */
  public long count(@Nonnull final String index, @Nullable final JsonNode filter)
      throws IOException {
    final ObjectNode body = object();
    if (filter != null) {
      body.set("query", filter);
    }
    return json("POST", "/" + index + "/_count", body).path("count").asLong();
  }

  /** Index names an expression resolves to, in the order the cluster lists them. */
  @Nonnull
  public List<String> resolveIndices(@Nonnull final String expression) throws IOException {
    // Elasticsearch answers a missing name with 404, OpenSearch with an empty result.
    final JsonNode response = jsonOrNull("GET", "/_resolve/index/" + expression, null);
    final List<String> names = new ArrayList<>();
    if (response != null) {
      response.path("indices").forEach(index -> names.add(index.path("name").asText()));
    }
    return names;
  }

  /** {@code index -> is_write_index} for every index behind {@code alias}, or empty. */
  @Nonnull
  public List<AliasMember> aliasMembers(@Nonnull final String alias) throws IOException {
    final JsonNode response = jsonOrNull("GET", "/_alias/" + alias, null);
    final List<AliasMember> members = new ArrayList<>();
    if (response == null) {
      return members;
    }
    response
        .fields()
        .forEachRemaining(
            entry -> {
              final JsonNode aliasNode = entry.getValue().path("aliases").path(alias);
              members.add(
                  new AliasMember(entry.getKey(), aliasNode.path("is_write_index").asBoolean()));
            });
    return members;
  }

  /**
   * Backing indices of a data stream in generation order (the last is the write index), or null
   * when the name is not a data stream: Elasticsearch answers 404, OpenSearch answers 400 when the
   * name is an alias.
   */
  @Nullable
  public List<String> dataStreamIndices(@Nonnull final String dataStream) throws IOException {
    final Answer answer = answer("GET", "/_data_stream/" + dataStream, null, "400", "404");
    if (answer.status() == 400 || answer.status() == 404) {
      return null;
    }
    final JsonNode streams = answer.body().path("data_streams");
    if (streams.isEmpty()) {
      return null;
    }
    final List<String> names = new ArrayList<>();
    streams.get(0).path("indices").forEach(index -> names.add(index.path("index_name").asText()));
    return names;
  }

  /**
   * The task's status, or null when the cluster no longer knows it (for example after a restart).
   */
  @Nullable
  public JsonNode task(@Nonnull final String taskId) throws IOException {
    return jsonOrNull("GET", "/_tasks/" + taskId, null);
  }

  /** Running tasks of the given action pattern, with their headers, for lost-id recovery. */
  @Nonnull
  public JsonNode runningTasks(@Nonnull final String actions) throws IOException {
    final JsonNode response = json("GET", "/_tasks?actions=" + actions + "&detailed=true", null);
    if (!response.path("node_failures").isEmpty() || !response.path("task_failures").isEmpty()) {
      throw new IOException("Task listing reported failures: " + response);
    }
    return response;
  }

  /** Deletes the index; an index that is already gone is fine, any other refusal throws. */
  public void deleteIndex(@Nonnull final String index) throws IOException {
    log.info("DELETE => /{}", index);
    answer("DELETE", "/" + index, null, "404");
  }

  @Nonnull
  public String engineLabel() {
    return client.getEngineType().name().toLowerCase(Locale.ROOT);
  }

  public boolean isOpenSearch() {
    return client.getEngineType().isOpenSearch();
  }

  /** One index behind an alias and whether it is the alias's write index. */
  public record AliasMember(String index, boolean writeIndex) {}
}
