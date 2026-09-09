package com.linkedin.metadata.utils.elasticsearch;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * The convention for naming search indices.
 *
 * <p>Every method that produces or parses an index name takes an {@link OperationFingerprint}: the
 * index-name prefix is resolved per operation via an {@link IndexPrefixResolver}, so a single
 * convention instance serves every operation. In OSS the prefix is a static deploy-wide value; an
 * extension module may resolve it per operation (e.g. per-namespace index isolation) without
 * callers knowing how. {@code OperationContext} implements {@link OperationFingerprint}, so callers
 * holding a full context pass it straight through; bootstrap / test paths pass {@link
 * OperationFingerprint#EMPTY}.
 */
public interface IndexConvention {
  /** The prefix applied to index names for {@code operation}, or empty when there is none. */
  Optional<String> getPrefix(@Nonnull OperationFingerprint operation);

  @Nonnull
  String getIndexName(
      @Nonnull OperationFingerprint operation, Class<? extends RecordTemplate> documentClass);

  @Nonnull
  String getIndexName(@Nonnull OperationFingerprint operation, EntitySpec entitySpec);

  @Nonnull
  String getIndexName(@Nonnull OperationFingerprint operation, String baseIndexName);

  @Nonnull
  String getEntityIndexName(@Nonnull OperationFingerprint operation, String entityName);

  @Nonnull
  String getEntityIndexNameSemantic(@Nonnull OperationFingerprint operation, String entityName);

  @Nonnull
  String getEntityIndexNameV3(@Nonnull OperationFingerprint operation, String searchGroup);

  @Nonnull
  String getTimeseriesAspectIndexName(
      @Nonnull OperationFingerprint operation, String entityName, String aspectName);

  @Nonnull
  List<String> getAllEntityIndicesPatterns(@Nonnull OperationFingerprint operation);

  @Nonnull
  List<String> getV3EntityIndexPatterns(@Nonnull OperationFingerprint operation);

  @Nonnull
  String getAllTimeseriesAspectIndicesPattern(@Nonnull OperationFingerprint operation);

  /**
   * Returns entity index patterns for cleanup operations. This method considers both V2 and V3
   * patterns based on their cleanup configuration flags.
   *
   * @param operation operation whose prefix scopes the returned patterns
   * @param entityIndexConfiguration The configuration containing V2/V3 enable and cleanup flags
   * @return List of index patterns that should be included in cleanup operations
   */
  @Nonnull
  List<String> getEntityIndicesCleanupPatterns(
      @Nonnull OperationFingerprint operation,
      @Nonnull EntityIndexConfiguration entityIndexConfiguration);

  /**
   * Inverse of getEntityIndexName
   *
   * @param operation operation whose prefix is stripped from {@code indexName}
   * @param indexName The index name to parse
   * @return a string, the entity name that that index is for, or empty if one cannot be extracted
   */
  Optional<String> getEntityName(@Nonnull OperationFingerprint operation, String indexName);

  /**
   * Inverse of getEntityIndexNameSemantic
   *
   * @param operation operation whose prefix is stripped from {@code semanticIndexName}
   * @param semanticIndexName The semantic index name to parse
   * @return a string, the entity name that that index is for, or empty if one cannot be extracted
   */
  Optional<String> getEntityNameSemantic(
      @Nonnull OperationFingerprint operation, String semanticIndexName);

  /**
   * Inverse of getEntityIndexName
   *
   * @param operation operation whose prefix is stripped from {@code timeseriesAspectIndexName}
   * @param timeseriesAspectIndexName The index name to parse
   * @return a pair of strings, the entity name and the aspect name that that index is for, or empty
   *     if one cannot be extracted
   */
  Optional<Pair<String, String>> getEntityAndAspectName(
      @Nonnull OperationFingerprint operation, String timeseriesAspectIndexName);

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
   * @param operation operation whose prefix the index name is expected to carry
   * @param indexName the index name to check
   * @return true if the index name matches the v2 entity pattern
   */
  boolean isV2EntityIndex(@Nonnull OperationFingerprint operation, @Nonnull String indexName);

  /**
   * Checks if the given index name matches the v3 entity naming pattern. V3 entity indices should
   * contain "index_v3" in their name.
   *
   * @param operation operation whose prefix the index name is expected to carry
   * @param indexName the index name to check
   * @return true if the index name matches the v3 entity pattern
   */
  boolean isV3EntityIndex(@Nonnull OperationFingerprint operation, @Nonnull String indexName);

  /**
   * Checks if the given index name is a semantic entity index. Semantic entity indices should
   * contain "_semantic" in their name.
   *
   * @param operation operation whose prefix the index name is expected to carry
   * @param indexName the index name to check
   * @return true if the index name is a semantic entity index
   */
  boolean isSemanticEntityIndex(@Nonnull OperationFingerprint operation, @Nonnull String indexName);

  // Prefix-INDEPENDENT type checks for an already-resolved index name — is it a V2 / V3 / semantic
  // entity index, regardless of any deployment or per-operation (e.g. per-namespace) prefix? Use
  // these in index-build / settings paths that receive a fully-resolved name and only need its
  // TYPE. The operation-scoped overloads above are for prefix-scoped matching (e.g. orphan cleanup
  // that must touch only the current operation's indices) and would wrongly reject a name whose
  // prefix came from a different operation than the caller's.

  /** True if {@code indexName} is a V2 entity index, ignoring any prefix. */
  default boolean isV2EntityIndexType(@Nonnull String indexName) {
    return indexName.endsWith("index_v2") && indexName.length() > "index_v2".length();
  }

  /** True if {@code indexName} is a V3 entity index, ignoring any prefix. */
  default boolean isV3EntityIndexType(@Nonnull String indexName) {
    return indexName.endsWith("index_v3") && indexName.length() > "index_v3".length();
  }

  /** True if {@code indexName} is a V2 semantic entity index, ignoring any prefix. */
  default boolean isSemanticEntityIndexType(@Nonnull String indexName) {
    return indexName.endsWith("index_v2_semantic")
        && indexName.length() > "index_v2_semantic".length();
  }
}
