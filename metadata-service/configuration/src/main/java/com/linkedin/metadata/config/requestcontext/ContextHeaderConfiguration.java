package com.linkedin.metadata.config.requestcontext;

import lombok.Data;

@Data
public class ContextHeaderConfiguration {
  /**
   * Label when a configured dimension is missing from the header. Defaults: {@code
   * datahub.requestContext.contextHeader} in {@code application.yaml}.
   */
  private String unspecifiedLabel;

  /**
   * Label when a value is present but not in the allowlist for that key (only applies when that key
   * has a non-empty allowlist derived from {@link #getValueAllowlistsJson()}). Defaults: same YAML
   * block as {@link #unspecifiedLabel}.
   */
  private String otherLabel;

  /**
   * Maximum length for sanitized context values. Defaults: same YAML block as {@link
   * #unspecifiedLabel}.
   */
  private int maxValueLength;

  /**
   * JSON array of allowlist objects: {@code [{"key":"...","values":["..."]}, ...]}. Each object has
   * string {@code key} (header name) and {@code values} (string array of allowed sanitized tokens).
   * Omitted, null, or empty {@code values} means unrestricted for that key. Duplicate {@code key}
   * entries merge (union). The {@code X-DataHub-Context} header itself is unordered; Micrometer tag
   * order is lexicographic by key name (not JSON array order). Defaults and overrides: {@code
   * datahub.requestContext.contextHeader.valueAllowlistsJson} in {@code application.yaml} or your
   * platform’s property binding.
   */
  private String valueAllowlistsJson;
}
