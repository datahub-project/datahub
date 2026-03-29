package io.datahubproject.metadata.context.request;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Parses {@value com.linkedin.metadata.Constants#DATAHUB_CONTEXT_HEADER_NAME} for low-cardinality
 * metric and trace labels. Format: {@code key=value} pairs separated by {@code ;}; segment order is
 * not significant (e.g. {@code skill=datahub-audit;caller=claude-code} is equivalent to {@code
 * caller=claude-code;skill=datahub-audit}).
 *
 * <p>Which keys are accepted and optional value allowlists come from {@link
 * DataHubContextParsePolicy} (typically from {@code datahub.requestContext.contextHeader} in {@code
 * application.yaml}). Values are normalized to lowercase Prometheus-safe tokens (length-capped).
 * Unknown keys are ignored. When {@link DataHubContextKeyRule#restrictsValues()} is true, values
 * not in the allowlist are recorded as {@link DataHubContextParsePolicy#getOtherLabel()}.
 *
 * <p>{@link RequestContext} attaches parsed values to per-request metrics and tracing.
 */
public final class DataHubContextParser {

  /** Default label when a dimension is absent or could not be parsed safely (configurable). */
  public static final String UNSPECIFIED = "unspecified";

  public static final int DEFAULT_MAX_VALUE_LENGTH = 48;

  private DataHubContextParser() {}

  /**
   * Parses using the policy currently installed via {@link DataHubContextRulesHolder} (defaults if
   * never set).
   */
  public static Parsed parse(@Nullable String rawHeader) {
    return parse(rawHeader, DataHubContextRulesHolder.get());
  }

  public static Parsed parse(@Nullable String rawHeader, DataHubContextParsePolicy policy) {
    if (policy == null) {
      policy = DataHubContextParsePolicy.defaults();
    }
    List<DataHubContextKeyRule> rules = policy.getRules();
    if (rules == null || rules.isEmpty()) {
      policy = DataHubContextParsePolicy.defaults();
      rules = policy.getRules();
    }

    String unspecified = policy.getUnspecifiedLabel();
    String other = policy.getOtherLabel();
    int maxLen = Math.max(1, policy.getMaxValueLength());

    Map<String, String> firstRawByHeaderKey = new HashMap<>();
    if (rawHeader != null && !rawHeader.isBlank()) {
      Set<String> knownKeys =
          rules.stream().map(DataHubContextKeyRule::getHeaderKey).collect(Collectors.toSet());
      for (String segment : rawHeader.split(";")) {
        String part = segment.trim();
        if (part.isEmpty()) {
          continue;
        }
        int eq = part.indexOf('=');
        if (eq <= 0 || eq == part.length() - 1) {
          continue;
        }
        String key = part.substring(0, eq).trim().toLowerCase(Locale.ROOT);
        if (!knownKeys.contains(key) || firstRawByHeaderKey.containsKey(key)) {
          continue;
        }
        firstRawByHeaderKey.put(key, part.substring(eq + 1).trim());
      }
    }

    LinkedHashMap<String, String> metricTagToValue = new LinkedHashMap<>();
    for (DataHubContextKeyRule rule : rules) {
      String rawVal = firstRawByHeaderKey.get(rule.getHeaderKey());
      String finalValue;
      if (rawVal == null) {
        finalValue = unspecified;
      } else {
        String sanitized = sanitizeValue(rawVal, maxLen, unspecified);
        if (rule.restrictsValues() && !rule.getAllowedValues().contains(sanitized)) {
          finalValue = other;
        } else {
          finalValue = sanitized;
        }
      }
      metricTagToValue.put(rule.getMetricTagName(), finalValue);
    }

    return new Parsed(policy, metricTagToValue);
  }

  /**
   * Sanitizes a single token for use as a metric label value. Invalid or empty input yields {@code
   * unspecifiedToken}.
   */
  public static String sanitizeValue(String raw, int maxLength, String unspecifiedToken) {
    if (raw == null) {
      return unspecifiedToken;
    }
    int cap = Math.max(1, maxLength);
    String s = raw.trim().toLowerCase(Locale.ROOT);
    if (s.isEmpty()) {
      return unspecifiedToken;
    }
    StringBuilder sb = new StringBuilder(Math.min(s.length(), cap));
    for (int i = 0; i < s.length() && sb.length() < cap; i++) {
      char c = s.charAt(i);
      if ((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_' || c == '-') {
        sb.append(c);
      } else {
        sb.append('_');
      }
    }
    String out = sb.toString().replaceAll("_+", "_");
    if (out.isEmpty() || "_".equals(out)) {
      return unspecifiedToken;
    }
    if (out.startsWith("_")) {
      out = out.substring(1);
    }
    if (out.endsWith("_")) {
      out = out.substring(0, out.length() - 1);
    }
    return out.isEmpty() ? unspecifiedToken : out;
  }

  @Value
  public static class Parsed {
    DataHubContextParsePolicy policy;
    LinkedHashMap<String, String> metricTagToValue;

    public String getSkill() {
      return metricTagToValue.getOrDefault(
          MetricUtils.TAG_AGENT_SKILL, policy.getUnspecifiedLabel());
    }

    public String getCaller() {
      return metricTagToValue.getOrDefault(
          MetricUtils.TAG_AGENT_CALLER, policy.getUnspecifiedLabel());
    }

    public String getForHeaderKey(String headerKey) {
      if (headerKey == null) {
        return policy.getUnspecifiedLabel();
      }
      String hk = headerKey.trim().toLowerCase(Locale.ROOT);
      for (DataHubContextKeyRule r : policy.getRules()) {
        if (r.getHeaderKey().equals(hk)) {
          return metricTagToValue.getOrDefault(r.getMetricTagName(), policy.getUnspecifiedLabel());
        }
      }
      return policy.getUnspecifiedLabel();
    }

    /** Alternating Micrometer tag names and values in stable policy order. */
    public String[] flatMicrometerTagPairs() {
      String[] out = new String[metricTagToValue.size() * 2];
      int i = 0;
      for (Map.Entry<String, String> e : metricTagToValue.entrySet()) {
        out[i++] = e.getKey();
        out[i++] = e.getValue();
      }
      return out;
    }
  }
}
