package com.linkedin.metadata.search.semantic;

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
  public Optional<String> getPrefix() {
    return delegate.getPrefix();
  }

  @Override
  @Nonnull
  public String getIndexName(Class<? extends RecordTemplate> documentClass) {
    return delegate.getIndexName(documentClass);
  }

  @Override
  @Nonnull
  public String getIndexName(EntitySpec entitySpec) {
    return delegate.getIndexName(entitySpec);
  }

  @Override
  @Nonnull
  public String getIndexName(String baseIndexName) {
    return delegate.getIndexName(baseIndexName);
  }

  @Override
  @Nonnull
  public String getEntityIndexName(String entityName) {
    // This is the key method - append _semantic to entity index names
    return appendSemanticSuffix(delegate.getEntityIndexName(entityName));
  }

  @Nonnull
  @Override
  public String getEntityIndexNameSemantic(String entityName) {
    return delegate.getEntityIndexNameSemantic(entityName);
  }

  @Override
  @Nonnull
  public String getTimeseriesAspectIndexName(String entityName, String aspectName) {
    return delegate.getTimeseriesAspectIndexName(entityName, aspectName);
  }

  @Nonnull
  @Override
  public String getEntityIndexNameV3(String searchGroup) {
    return delegate.getEntityIndexNameV3(searchGroup);
  }

  @Nonnull
  @Override
  public List<String> getAllEntityIndicesPatterns() {
    return delegate.getAllEntityIndicesPatterns();
  }

  @Nonnull
  @Override
  public List<String> getV3EntityIndexPatterns() {
    return delegate.getV3EntityIndexPatterns();
  }

  @Nonnull
  @Override
  public List<String> getEntityIndicesCleanupPatterns(
      @Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    return delegate.getEntityIndicesCleanupPatterns(entityIndexConfiguration);
  }

  @Override
  public boolean isV2EntityIndex(@Nonnull String indexName) {
    return false;
  }

  @Override
  public boolean isV3EntityIndex(@Nonnull String indexName) {
    return false;
  }

  @Override
  public boolean isSemanticEntityIndex(@Nonnull String indexName) {
    return delegate.isSemanticEntityIndex(indexName);
  }

  @Override
  @Nonnull
  public String getAllTimeseriesAspectIndicesPattern() {
    return delegate.getAllTimeseriesAspectIndicesPattern();
  }

  @Override
  public Optional<String> getEntityName(String indexName) {
    return delegate.getEntityName(indexName);
  }

  @Override
  public Optional<String> getEntityNameSemantic(String semanticIndexName) {
    return delegate.getEntityNameSemantic(semanticIndexName);
  }

  @Override
  public Optional<Pair<String, String>> getEntityAndAspectName(String timeseriesAspectIndexName) {
    return delegate.getEntityAndAspectName(timeseriesAspectIndexName);
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
