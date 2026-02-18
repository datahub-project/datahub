package com.linkedin.metadata.utils.elasticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/** The convention for naming search indices */
public interface IndexConvention {
  Optional<String> getPrefix();

  @Nonnull
  String getIndexName(Class<? extends RecordTemplate> documentClass);

  @Nonnull
  String getIndexName(EntitySpec entitySpec);

  @Nonnull
  String getIndexName(String baseIndexName);

  @Nonnull
  String getEntityIndexName(String entityName);

  @Nonnull
  String getEntityIndexNameSemantic(String entityName);

  @Nonnull
  String getEntityIndexNameV3(String searchGroup);

  @Nonnull
  String getTimeseriesAspectIndexName(String entityName, String aspectName);

  @Nonnull
  List<String> getAllEntityIndicesPatterns();

  @Nonnull
  List<String> getV3EntityIndexPatterns();

  @Nonnull
  String getAllTimeseriesAspectIndicesPattern();

  /**
   * Returns entity index patterns for cleanup operations. This method considers both V2 and V3
   * patterns based on their cleanup configuration flags.
   *
   * @param entityIndexConfiguration The configuration containing V2/V3 enable and cleanup flags
   * @return List of index patterns that should be included in cleanup operations
   */
  @Nonnull
  List<String> getEntityIndicesCleanupPatterns(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration);

  /**
   * Inverse of getEntityIndexName
   *
   * @param indexName The index name to parse
   * @return a string, the entity name that that index is for, or empty if one cannot be extracted
   */
  Optional<String> getEntityName(String indexName);

  /**
   * Inverse of getEntityIndexNameSemantic
   *
   * @param semanticIndexName The semantic index name to parse
   * @return a string, the entity name that that index is for, or empty if one cannot be extracted
   */
  Optional<String> getEntityNameSemantic(String semanticIndexName);

  /**
   * Inverse of getEntityIndexName
   *
   * @param timeseriesAspectIndexName The index name to parse
   * @return a pair of strings, the entity name and the aspect name that that index is for, or empty
   *     if one cannot be extracted
   */
  Optional<Pair<String, String>> getEntityAndAspectName(String timeseriesAspectIndexName);

  @Nonnull
  String getIdHashAlgo();

  /**
   * Given the URN generate the document id for entity indices
   *
   * @param entityUrn the entity which the document belongs
   * @return document id
   */
  @Nonnull
  String getEntityDocumentId(Urn entityUrn);

  /**
   * Checks if the given index name matches the v2 entity naming pattern. V2 entity indices should
   * contain "index_v2" in their name.
   *
   * @param indexName the index name to check
   * @return true if the index name matches the v2 entity pattern
   */
  boolean isV2EntityIndex(@Nonnull String indexName);

  /**
   * Checks if the given index name matches the v3 entity naming pattern. V3 entity indices should
   * contain "index_v3" in their name.
   *
   * @param indexName the index name to check
   * @return true if the index name matches the v3 entity pattern
   */
  boolean isV3EntityIndex(@Nonnull String indexName);

  /**
   * Checks if the given index name is a semantic entity index. Semantic entity indices should
   * contain "_semantic" in their name.
   *
   * @param indexName the index name to check
   * @return true if the index name is a semantic entity index
   */
  boolean isSemanticEntityIndex(@Nonnull String indexName);
}
