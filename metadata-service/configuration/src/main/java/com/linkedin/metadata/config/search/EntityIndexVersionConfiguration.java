package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class EntityIndexVersionConfiguration {
  private boolean enabled;
  private boolean cleanup;
  private String analyzerConfig;
  private String mappingConfig;
  private Integer maxFieldsLimit;

  /**
   * When true, keyword search/browse reads V3 indices even if V2 is still enabled (dual-write
   * cutover). Ignored when V3 is disabled.
   */
  private boolean keywordReadEnabled;

  /**
   * When true, semantic (kNN) search reads document vectors from V3 indices instead of the V2
   * semantic indices. Separate from {@link #keywordReadEnabled} so keyword and semantic reads cut
   * over independently. Ignored when V3 is disabled.
   */
  private boolean semanticReadEnabled;

  /** V2 only: coalesce multiple updates to the same (urn, aspect) within a batch. */
  private boolean coalesceBatchUpdates;

  /**
   * V2 only. Search V3 always hashes document ids, so it intentionally has no equivalent setting —
   * do not add one here for v3.
   */
  private String idHashAlgo;

  /** V2 only: schema field document id strategy. */
  private DocIdsConfiguration docIds;

  /** V2 only: analyzer tokenizer override for the legacy settings builder. */
  private String mainTokenizer;

  public boolean isSchemaFieldDocIdHashEnabled() {
    return docIds != null
        && docIds.getSchemaField() != null
        && docIds.getSchemaField().isHashIdEnabled();
  }
}
