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
   * When true, an install that already reads V3 with V2 off records the cutover and does not move
   * reads back to the V2 indices. Ignored unless every paired V3 index exists and has at least as
   * many documents as its V2 index. The retired stamp written on that start is what later starts
   * trust.
   */
  private boolean cutoverAcknowledged;

  @Builder.Default
  private SearchGroupIndexSettingsConfiguration defaultSettings =
      new SearchGroupIndexSettingsConfiguration();

  @Builder.Default
  private java.util.Map<String, SearchGroupIndexSettingsConfiguration> groups =
      new java.util.HashMap<>();

  /**
   * When true, full-text keyword search on V3 reranks the rows of entity types that have vectors
   * (documents by default) by combined keyword and vector score, among the positions those rows
   * already hold. Needs semantic search reading the same V3 indices as keyword search. Ignored when
   * V3 is disabled.
   */
  private boolean hybridReadEnabled;

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
