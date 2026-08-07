package com.linkedin.metadata.utils.elasticsearch;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parses {@code response.failures[]} from a raw ES/OpenSearch {@code GET _tasks/<id>} JSON body.
 * Never throws — logging must not fail a reindex.
 */
public final class TaskFailureParser {

  private static final Logger log = LoggerFactory.getLogger(TaskFailureParser.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_FAILURES_TO_PARSE = 10;
  private static final int MAX_REASON_LENGTH = 500;
  private static final int MAX_RAW_FALLBACK_LENGTH = 4000;
  private static final int DEFAULT_LOG_LIMIT = 5;

  private TaskFailureParser() {}

  /**
   * Parses document failures from raw task JSON. Details are capped at {@value
   * #MAX_FAILURES_TO_PARSE}; {@link TaskFailureParseResult#totalCount()} is the full array length.
   * On malformed JSON or parse exceptions, returns empty details with truncated {@code rawFallback}
   * — never throws.
   */
  @Nonnull
  public static TaskFailureParseResult parse(@Nullable String rawJson) {
    if (rawJson == null || rawJson.isEmpty()) {
      return TaskFailureParseResult.EMPTY;
    }
    try {
      JsonNode root = MAPPER.readTree(rawJson);
      JsonNode failures = root.path("response").path("failures");
      if (failures.isMissingNode() || !failures.isArray()) {
        return TaskFailureParseResult.EMPTY;
      }
      int totalCount = failures.size();
      List<TaskFailureDetail> result = new ArrayList<>();
      for (JsonNode failure : failures) {
        if (result.size() >= MAX_FAILURES_TO_PARSE) {
          break;
        }
        result.add(parseOne(failure));
      }
      return new TaskFailureParseResult(List.copyOf(result), totalCount, null);
    } catch (Exception e) {
      log.warn("Failed to parse task failure JSON; retaining truncated raw: {}", e.toString());
      return new TaskFailureParseResult(List.of(), 0, truncateRaw(rawJson));
    }
  }

  /**
   * Formats document failures for log lines. Never throws. Count reflects {@code failures.size()};
   * detail lines are capped at {@code limit}.
   */
  @Nonnull
  public static String formatForLog(@Nullable List<TaskFailureDetail> failures, int limit) {
    int total = failures == null ? 0 : failures.size();
    return formatForLog(failures, limit, total);
  }

  /**
   * Formats document failures for log lines. Never throws. {@code totalCount} is used for {@code
   * documentFailures=N} (may exceed list size when parse was capped); detail lines are capped at
   * {@code limit}.
   */
  @Nonnull
  public static String formatForLog(
      @Nullable List<TaskFailureDetail> failures, int limit, int totalCount) {
    try {
      if (failures == null || failures.isEmpty()) {
        return "";
      }
      int count = Math.max(totalCount, failures.size());
      int detailLimit = Math.max(0, limit);
      StringBuilder sb = new StringBuilder();
      sb.append(String.format("documentFailures=%d", count));
      failures.stream()
          .limit(detailLimit)
          .forEach(
              f ->
                  sb.append(
                      String.format(
                          "%n  FAILED doc=%s index=%s cause=%s: %s",
                          f.documentId(), f.index(), f.causeType(), f.causeReason())));
      return sb.toString();
    } catch (Exception e) {
      return "";
    }
  }

  /**
   * Formats a parse result for logs. Prefer structured details; if those are empty but {@code
   * rawFallback} is present, print the truncated raw JSON. Never throws.
   */
  @Nonnull
  public static String formatForLog(@Nullable TaskFailureParseResult parseResult, int limit) {
    try {
      if (parseResult == null) {
        return "";
      }
      if (!parseResult.details().isEmpty()) {
        return formatForLog(parseResult.details(), limit, parseResult.totalCount());
      }
      String raw = parseResult.rawFallback();
      if (raw != null && !raw.isBlank()) {
        return String.format("%n  RAW task response (parse incomplete): %s", raw);
      }
      return "";
    } catch (Exception e) {
      return "";
    }
  }

  @Nonnull
  private static TaskFailureDetail parseOne(@Nonnull JsonNode failure) {
    return new TaskFailureDetail(
        failure.path("index").asText("unknown"),
        failure.path("id").asText("unknown"),
        failure.path("cause").path("type").asText("unknown"),
        truncate(failure.path("cause").path("reason").asText(""), MAX_REASON_LENGTH),
        failure.path("shard").asInt(-1));
  }

  @Nonnull
  private static String truncate(@Nonnull String value, int maxLen) {
    return value.length() <= maxLen ? value : value.substring(0, maxLen);
  }

  @Nullable
  static String truncateRaw(@Nullable String rawJson) {
    if (rawJson == null || rawJson.isEmpty()) {
      return null;
    }
    return truncate(rawJson, MAX_RAW_FALLBACK_LENGTH);
  }
}
