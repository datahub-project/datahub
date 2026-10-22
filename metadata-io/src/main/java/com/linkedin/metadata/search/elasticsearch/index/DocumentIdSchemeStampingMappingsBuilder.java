package com.linkedin.metadata.search.elasticsearch.index;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.V3DocumentIdScheme;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Wraps a {@link MappingsBuilder} whose indices hold hashed document ids and records the running
 * {@link V3DocumentIdScheme} on every index mapping it emits.
 *
 * <p>The stamp reaches an index only when it is created from these mappings; put-mapping paths
 * strip it (see {@code ESIndexBuilder#applyMappings}), so an existing index built under another
 * scheme keeps reading as one that needs a rebuild. Mapping comparison ignores {@code _meta}, so
 * the stamp never causes a reindex on its own.
 */
public class DocumentIdSchemeStampingMappingsBuilder implements MappingsBuilder {

  private final MappingsBuilder delegate;
  private final V3DocumentIdScheme scheme;

  public DocumentIdSchemeStampingMappingsBuilder(
      @Nonnull final MappingsBuilder delegate, @Nonnull final V3DocumentIdScheme scheme) {
    this.delegate = delegate;
    this.scheme = scheme;
  }

  @Override
  public Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    return stamped(delegate.getIndexMappings(opContext, structuredProperties));
  }

  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    return stamped(delegate.getIndexMappingsWithNewStructuredProperty(opContext, urn, property));
  }

  @Override
  public Map<String, Object> getIndexMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    return delegate.getIndexMappingsForStructuredProperty(properties);
  }

  @Override
  public Map<String, Object> getIndexMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    return delegate.getIndexMappings(entityRegistry, entitySpec);
  }

  private List<IndexMapping> stamped(@Nonnull final Collection<IndexMapping> indexMappings) {
    return indexMappings.stream()
        .map(
            indexMapping ->
                IndexMapping.builder()
                    .indexName(indexMapping.getIndexName())
                    .mappings(scheme.stampedMappings(indexMapping.getMappings()))
                    .build())
        .collect(Collectors.toList());
  }
}
