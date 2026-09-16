package com.linkedin.metadata.search.elasticsearch.index;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * No-op implementation of MappingsBuilder that returns empty collections. Used when no mappings
 * builder is available or when mappings are not needed.
 */
@Slf4j
public class NoOpMappingsBuilder implements MappingsBuilder {

  @Override
  public Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    log.debug("NoOpMappingsBuilder: returning empty collection for getIndexMappings");
    return Collections.emptyList();
  }

  @Override
  public Collection<IndexMapping> getIndexMappings(@Nonnull OperationContext opContext) {
    log.debug("NoOpMappingsBuilder: returning empty collection for getMappings");
    return Collections.emptyList();
  }

  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    log.debug(
        "NoOpMappingsBuilder: returning empty collection for getIndexMappingsWithNewStructuredProperty");
    return Collections.emptyList();
  }

  @Override
  public Map<String, Object> getIndexMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    log.debug("NoOpMappingsBuilder: returning empty map for getMappingsForStructuredProperty");
    return Collections.emptyMap();
  }

  @Override
  public Map<String, Object> getIndexMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    log.debug("NoOpMappingsBuilder: returning empty map for getMappings");
    return Collections.emptyMap();
  }
}
