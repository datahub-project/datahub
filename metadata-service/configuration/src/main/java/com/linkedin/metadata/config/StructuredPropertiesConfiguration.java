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
   * Max UTF-8 bytes for string-backed structured property values ({@code string}, {@code
   * rich_text}, {@code date}, {@code urn}). Keyword mappings derive a byte-safe character {@code
   * ignore_above} from this limit ({@code keywordMaxLength / 4}). Default is set in {@code
   * application.yaml}.
   */
  private int keywordMaxLength;
}
