package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Strategy interface for handling different update indices versions (v2 vs v3) in
 * UpdateIndicesService. This allows the service to support both mapping versions simultaneously and
 * delegate update indices operations to the appropriate strategy.
 */
public interface UpdateIndicesStrategy {

  /**
   * Optimized batch processing method that takes pre-grouped events by URN. This allows for maximum
   * efficiency by avoiding redundant grouping operations. V3 implementations should prefer this
   * method over the ungrouped version.
   *
   * @param opContext the operation context
   * @param groupedEvents events grouped by URN, preserving order
   */
  void processBatch(
      @Nonnull OperationContext opContext,
      @Nonnull Map<Urn, List<MCLItem>> groupedEvents,
      boolean structuredPropertiesHookEnabled);

  /**
   * Gets index mappings for a given operation context.
   *
   * @param opContext the operation context
   * @return collection of index mappings
   */
  Collection<MappingsBuilder.IndexMapping> getIndexMappings(@Nonnull OperationContext opContext);

  /**
   * Gets index mappings with new structured properties.
   *
   * @param opContext the operation context
   * @param urn the URN of the structured property
   * @param property the structured property definition
   * @return collection of index mappings
   */
  Collection<MappingsBuilder.IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property);

  /**
   * Updates index mappings for structured property changes. This method handles the mapping updates
   * when structured property definitions change, ensuring that Elasticsearch indices are updated
   * with the new field mappings.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param entitySpec the entity specification
   * @param aspectSpec the aspect specification
   * @param newValue the new aspect value
   * @param oldValue the old aspect value
   */
  void updateIndexMappings(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull EntitySpec entitySpec,
      @Nonnull AspectSpec aspectSpec,
      @Nonnull Object newValue,
      @Nullable Object oldValue);

  /**
   * Checks if this mapping strategy is enabled.
   *
   * @return true if enabled, false otherwise
   */
  boolean isEnabled();
}
