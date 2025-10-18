package com.linkedin.metadata.search.elasticsearch.index;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;

/**
 * Interface for building Elasticsearch mappings for entities. This interface defines the contract
 * for classes that build Elasticsearch index mappings.
 */
public interface MappingsBuilder {

  /**
   * Builds mappings for an entity registry and a collection of structured properties.
   *
   * @param structuredProperties structured properties for the entity
   * @return mappings
   */
  Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties);

  /**
   * Builds mappings for an entity registry.
   *
   * @return mappings
   */
  Collection<IndexMapping> getMappings(@Nonnull OperationContext opContext);

  Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property);

  /**
   * Builds mappings for structured properties.
   *
   * @param properties collection of structured property definitions
   * @return mappings for structured properties
   */
  Map<String, Object> getMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties);

  /**
   * Method to get mappings for a single entity spec. This method is used by
   * ESUtils.buildSearchableFieldTypes to extract field types from mappings.
   *
   * @param entityRegistry entity registry
   * @param entitySpec entity spec to get mappings for
   * @return mappings for the entity spec
   */
  Map<String, Object> getMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec);

  @Builder
  @Data
  class IndexMapping {
    @Nonnull String indexName;
    @Nonnull Map<String, Object> mappings;
  }
}
