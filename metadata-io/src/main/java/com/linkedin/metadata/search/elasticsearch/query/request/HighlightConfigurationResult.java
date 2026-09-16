package com.linkedin.metadata.search.elasticsearch.query.request;

import java.util.Set;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

/**
 * Result of applying highlight field configuration. Contains both the fields to highlight and which
 * fields were explicitly configured.
 */
@Getter
@Builder
public class HighlightConfigurationResult {

  /** The final set of fields that should be highlighted */
  @NonNull private final Set<String> fieldsToHighlight;

  /**
   * Fields that were explicitly configured via 'add' or 'replace' operations. These fields should
   * not have field expansion applied.
   */
  @NonNull private final Set<String> explicitlyConfiguredFields;
}
