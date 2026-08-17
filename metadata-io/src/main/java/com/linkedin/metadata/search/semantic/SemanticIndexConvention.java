package com.linkedin.metadata.search.semantic;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.util.Pair;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Index convention wrapper that applies the semantic search naming convention by appending
 * _semantic suffix to entity index names. This ensures that any index name resolution follows the
 * semantic search index structure.
 *
 * <p>One common use case is in {@link
 * com.linkedin.metadata.utils.SearchUtil#transformFilterForEntities} where virtual filters like
 * _entityType need to reference semantic indices (e.g., datasetindex_v2_semantic) instead of base
 * keyword indices (e.g., datasetindex_v2).
 */
public class SemanticIndexConvention implements IndexConvention {

  private final IndexConvention delegate;

  public SemanticIndexConvention(@Nonnull IndexConvention delegate) {
    this.delegate =
        java.util.Objects.requireNonNull(delegate, "delegate IndexConvention cannot be null");
  }

  @Override
  public Optional<String> getPrefix(@Nonnull OperationFingerprint operation) {
    return delegate.getPrefix(operation);
  }

  @Override
  @Nonnull
  public String getIndexName(
      @Nonnull OperationFingerprint operation, Class<? extends RecordTemplate> documentClass) {
    return delegate.getIndexName(operation, documentClass);
  }

  @Override
  @Nonnull
  public String getIndexName(@Nonnull OperationFingerprint operation, EntitySpec entitySpec) {
    return delegate.getIndexName(operation, entitySpec);
  }

  @Override
  @Nonnull
  public String getIndexName(@Nonnull OperationFingerprint operation, String baseIndexName) {
    return delegate.getIndexName(operation, baseIndexName);
  }

  @Override
  @Nonnull
  public String getEntityIndexName(@Nonnull OperationFingerprint operation, String entityName) {
    // This is the key method - append _semantic to entity index names
    return appendSemanticSuffix(delegate.getEntityIndexName(operation, entityName));
  }

  @Nonnull
  @Override
  public String getEntityIndexNameSemantic(
      @Nonnull OperationFingerprint operation, String entityName) {
    return delegate.getEntityIndexNameSemantic(operation, entityName);
  }

  @Override
  @Nonnull
  public String getTimeseriesAspectIndexName(
      @Nonnull OperationFingerprint operation, String entityName, String aspectName) {
    return delegate.getTimeseriesAspectIndexName(operation, entityName, aspectName);
  }

  @Nonnull
  @Override
  public String getEntityIndexNameV3(@Nonnull OperationFingerprint operation, String searchGroup) {
    return delegate.getEntityIndexNameV3(operation, searchGroup);
  }

  @Nonnull
  @Override
  public List<String> getAllEntityIndicesPatterns(@Nonnull OperationFingerprint operation) {
    return delegate.getAllEntityIndicesPatterns(operation);
  }

  @Nonnull
  @Override
  public List<String> getV3EntityIndexPatterns(@Nonnull OperationFingerprint operation) {
    return delegate.getV3EntityIndexPatterns(operation);
  }

  @Nonnull
  @Override
  public List<String> getEntityIndicesCleanupPatterns(
      @Nonnull OperationFingerprint operation,
      @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return delegate.getEntityIndicesCleanupPatterns(operation, entityIndexConfiguration);
  }

  @Override
  public boolean isV2EntityIndex(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName) {
    return false;
  }

  @Override
  public boolean isV3EntityIndex(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName) {
    return false;
  }

  @Override
  public boolean isSemanticEntityIndex(
      @Nonnull OperationFingerprint operation, @Nonnull String indexName) {
    return delegate.isSemanticEntityIndex(operation, indexName);
  }

  @Override
  @Nonnull
  public String getAllTimeseriesAspectIndicesPattern(@Nonnull OperationFingerprint operation) {
    return delegate.getAllTimeseriesAspectIndicesPattern(operation);
  }

  @Override
  public Optional<String> getEntityName(@Nonnull OperationFingerprint operation, String indexName) {
    return delegate.getEntityName(operation, indexName);
  }

  @Override
  public Optional<String> getEntityNameSemantic(
      @Nonnull OperationFingerprint operation, String semanticIndexName) {
    return delegate.getEntityNameSemantic(operation, semanticIndexName);
  }

  @Override
  public Optional<Pair<String, String>> getEntityAndAspectName(
      @Nonnull OperationFingerprint operation, String timeseriesAspectIndexName) {
    return delegate.getEntityAndAspectName(operation, timeseriesAspectIndexName);
  }

  @Override
  @Nonnull
  public String getIdHashAlgo() {
    return delegate.getIdHashAlgo();
  }

  @Override
  @Nonnull
  public String getEntityDocumentId(Urn entityUrn) {
    return delegate.getEntityDocumentId(entityUrn);
  }

  /**
   * Appends the semantic index suffix to the provided base index name.
   *
   * @param baseIndex base index name (e.g., datasetindex_v2)
   * @return semantic index name (e.g., datasetindex_v2_semantic)
   */
  private static String appendSemanticSuffix(String baseIndex) {
    return baseIndex + "_semantic";
  }
}
