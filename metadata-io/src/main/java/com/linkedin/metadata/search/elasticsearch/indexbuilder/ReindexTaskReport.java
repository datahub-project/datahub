package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.google.common.annotations.VisibleForTesting;
import io.datahubproject.metadata.context.ObjectMapperContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A reindex task as stored by the search engine ({@code GET _tasks/<taskId>}): its counters, any
 * task-level error, and the per-document failures that the typed task APIs drop. Pure parsing, no
 * I/O, so the same rules apply whichever client fetched the JSON.
 *
 * @param completed the task's {@code completed} flag
 * @param counts the result counters, or the running task's status counters if it has no result
 * @param error the task-level error, if the task itself failed
 * @param failures the first {@value #MAX_PARSED_FAILURES} failures, across all slices
 * @param totalFailures how many failures the task reported, including those not parsed
 * @param rawFallback the start of the raw response when it could not be parsed, otherwise null
 */
public record ReindexTaskReport(
    @Nullable Object completed,
    @Nonnull Map<String, Object> counts,
    @Nullable Object error,
    @Nonnull List<Failure> failures,
    int totalFailures,
    @Nullable String rawFallback) {

  static final int MAX_PARSED_FAILURES = 20;
  static final int MAX_RAW_FALLBACK_CHARS = 1000;

  private static final Pattern FAILED_FIELD_PATTERN =
      Pattern.compile("field(?: \\[([^\\]]+)\\]|=\"([^\"]+)\")");
  private static final int VALUE_PREFIX_CHARS = 100;
  private static final int MAX_REASON_CHARS = 500;
  private static final String VALUE_PREVIEW_MARKER = "Preview of field's value: '";
  private static final String IMMENSE_TERM_PREFIX_MARKER = "The prefix of the first immense term";

  /** One failure from a reindex task's stored result, with its cause chain flattened. */
  public record Failure(
      String taskId,
      @Nullable Object status,
      @Nullable String docId,
      @Nullable Object shard,
      @Nullable String field,
      String causes) {

    static Failure parse(String taskId, Map<String, Object> failure) {
      // Bulk (write) failures carry the document id and cause; search failures carry the shard
      // and reason.
      StringBuilder causes = new StringBuilder();
      Object cause = failure.containsKey("cause") ? failure.get("cause") : failure.get("reason");
      while (cause instanceof Map) {
        Map<String, Object> c = (Map<String, Object>) cause;
        if (causes.length() > 0) {
          causes.append(" <- ");
        }
        causes.append(c.get("type")).append(": ").append(shortenValuePreview(c.get("reason")));
        cause = c.get("caused_by");
      }
      if (cause != null) {
        causes.append(shortenValuePreview(cause));
      }
      String field = null;
      Matcher m = FAILED_FIELD_PATTERN.matcher(causes);
      if (m.find()) {
        field = m.group(1) != null ? m.group(1) : m.group(2);
      }
      return new Failure(
          taskId,
          failure.get("status"),
          failure.get("id") == null ? null : String.valueOf(failure.get("id")),
          failure.get("shard"),
          field,
          causes.toString());
    }
  }

  /** Never throws: a response that can't be read comes back with {@link #rawFallback()} set. */
  @Nonnull
  public static ReindexTaskReport parse(@Nonnull String taskId, @Nullable String rawJson) {
    try {
      Map<String, Object> task = ObjectMapperContext.defaultMapper.readValue(rawJson, Map.class);
      Map<String, Object> result =
          (Map<String, Object>) task.getOrDefault("response", Collections.emptyMap());
      Map<String, Object> status =
          (Map<String, Object>)
              ((Map<String, Object>) task.getOrDefault("task", Collections.emptyMap()))
                  .getOrDefault("status", Collections.emptyMap());

      List<Map<String, Object>> raw =
          new ArrayList<>((List<Map<String, Object>>) result.getOrDefault("failures", List.of()));
      for (Map<String, Object> slice :
          (List<Map<String, Object>>) result.getOrDefault("slices", List.of())) {
        if (slice != null) {
          raw.addAll((List<Map<String, Object>>) slice.getOrDefault("failures", List.of()));
        }
      }
      List<Failure> failures = new ArrayList<>();
      for (Map<String, Object> failure : raw) {
        if (failures.size() >= MAX_PARSED_FAILURES) {
          break;
        }
        failures.add(Failure.parse(taskId, failure));
      }
      return new ReindexTaskReport(
          task.get("completed"),
          result.isEmpty() ? status : result,
          task.get("error"),
          List.copyOf(failures),
          raw.size(),
          null);
    } catch (Exception e) {
      // Malformed JSON, or a shape other than expected (the unchecked casts above).
      String text = String.valueOf(rawJson);
      return new ReindexTaskReport(
          null,
          Map.of(),
          null,
          List.of(),
          0,
          text.length() > MAX_RAW_FALLBACK_CHARS
              ? text.substring(0, MAX_RAW_FALLBACK_CHARS) + "... (truncated)"
              : text);
    }
  }

  /**
   * OpenSearch quotes the rejected value in its reasons, sometimes in full, so the quote is cut to
   * its first {@value #VALUE_PREFIX_CHARS} characters, the immense-term byte dump is dropped, and
   * the reason is capped.
   */
  @VisibleForTesting
  public static String shortenValuePreview(@Nullable Object reason) {
    String text = String.valueOf(reason);
    int immense = text.indexOf(IMMENSE_TERM_PREFIX_MARKER);
    if (immense >= 0) {
      text = text.substring(0, immense).trim();
    }
    int preview = text.indexOf(VALUE_PREVIEW_MARKER);
    if (preview >= 0) {
      int valueStart = preview + VALUE_PREVIEW_MARKER.length();
      if (text.length() - valueStart > VALUE_PREFIX_CHARS) {
        text = text.substring(0, valueStart + VALUE_PREFIX_CHARS) + "...' (truncated)";
      }
    }
    return text.length() > MAX_REASON_CHARS ? text.substring(0, MAX_REASON_CHARS) + "..." : text;
  }
}
