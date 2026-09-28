package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class StructuredPropertiesConfiguration {

  /** Whether structured properties mappings are applied */
  private boolean enabled;

  /** Whether structured property values can be written */
  private boolean writeEnabled;

  /** Whether structured property mappings are applied in system update job */
  private boolean systemUpdateEnabled;

  /**
   * When true (and {@link #systemUpdateEnabled} is also true), system-update reindexes entity
   * search indices whose structured-property field Elasticsearch types differ from the
   * definition-driven target (e.g. dynamic {@code float}/{@code long} vs intended {@code double}
   * for NUMBER). Both flags must be enabled for type-mismatch reindex to run.
   */
  private boolean typeMismatchReindexEnabled;

  /**
   * When true, structured property writes drop assignments whose definition entity does not exist,
   * logging a warning per dropped value. The write fails if no valid assignments remain.
   */
  private boolean dropMissingPropertyValuesWithWarning;

  /**
   * When true, string-backed structured property values that exceed {@link #keywordMaxLength} are
   * still written to primary storage, but are omitted from Elasticsearch / OpenSearch documents.
   * When false, those writes are rejected by {@code StructuredPropertiesValidator}.
   */
  private boolean dropOversizedKeywordValuesFromIndex;

  /**
   * UTF-8 byte threshold for string-backed structured property values ({@code string}, {@code
   * rich_text}, {@code date}, {@code urn}) used for write validation and search-document emission.
   * Yaml defaults this to Lucene's keyword term limit ({@code 32766} / {@code
   * ESUtils.KEYWORD_MAXLENGTH}); it is not a hard cap on primary-store size when {@link
   * #dropOversizedKeywordValuesFromIndex} is true. Keyword mappings derive a byte-safe character
   * {@code ignore_above} from this value ({@code keywordMaxLength / 4}).
   */
  private int keywordMaxLength;
}
