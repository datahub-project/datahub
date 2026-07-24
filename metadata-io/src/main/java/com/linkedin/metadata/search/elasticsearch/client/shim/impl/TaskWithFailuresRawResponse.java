package com.linkedin.metadata.search.elasticsearch.client.shim.impl;

import static com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.X_CONTENT_REGISTRY;

import com.linkedin.metadata.utils.elasticsearch.TaskFailureParser;
import com.linkedin.metadata.utils.elasticsearch.TaskResultWithFailures;
import java.io.IOException;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;

/**
 * Shared parse/404 handling for {@code getTaskWithFailures} on ES8 and OpenSearch2 shims. Keeps raw
 * JSON so {@code response.failures[]} survives typed-client stripping.
 */
final class TaskWithFailuresRawResponse {
  private TaskWithFailuresRawResponse() {}

  static Optional<TaskResultWithFailures> fromEntity(@Nullable HttpEntity entity)
      throws IOException {
    if (entity == null) {
      return Optional.empty();
    }
    return Optional.of(fromRawJson(EntityUtils.toString(entity, "UTF-8")));
  }

  static TaskResultWithFailures fromRawJson(String rawJson) throws IOException {
    GetTaskResponse parsed =
        GetTaskResponse.fromXContent(
            XContentType.JSON
                .xContent()
                .createParser(X_CONTENT_REGISTRY, LoggingDeprecationHandler.INSTANCE, rawJson));
    return new TaskResultWithFailures(parsed, TaskFailureParser.parse(rawJson));
  }

  /**
   * @return empty when HTTP status is 404; otherwise rethrow {@code exception}
   */
  static <E extends Exception> Optional<TaskResultWithFailures> emptyIfNotFound(
      int statusCode, E exception) throws E {
    if (statusCode == 404) {
      return Optional.empty();
    }
    throw exception;
  }
}
