package com.linkedin.metadata.service;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
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
   * Updates search indices for a collection of change events.
   *
   * @param opContext the operation context
   * @param events the collection of metadata change log items
   */
  void updateSearchIndices(
      @Nonnull OperationContext opContext, @Nonnull Collection<MCLItem> events);

  /**
   * Deletes search data for a given entity/aspect.
   *
   * @param opContext the operation context
   * @param urn the URN of the entity
   * @param entityName the entity name
   * @param aspectSpec the aspect specification
   * @param aspect the aspect data (nullable)
   * @param isKeyAspect whether this is a key aspect deletion
   * @param auditStamp the audit stamp
   */
  void deleteSearchData(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull String entityName,
      @Nonnull AspectSpec aspectSpec,
      @Nullable RecordTemplate aspect,
      @Nonnull Boolean isKeyAspect,
      @Nonnull AuditStamp auditStamp);

  /**
   * Updates timeseries fields for a collection of change events.
   *
   * @param opContext the operation context
   * @param events the collection of metadata change log items
   */
  void updateTimeseriesFields(
      @Nonnull OperationContext opContext, @Nonnull Collection<MCLItem> events);

  /**
   * Optimized batch processing method that takes pre-grouped events by URN. This allows for maximum
   * efficiency by avoiding redundant grouping operations. V3 implementations should prefer this
   * method over the ungrouped version.
   *
   * @param opContext the operation context
   * @param groupedEvents events grouped by URN, preserving order
   */
  default void processBatch(
      @Nonnull OperationContext opContext, @Nonnull Map<Urn, List<MCLItem>> groupedEvents) {
    // Default implementation ignores the events
  }

  /**
   * Updates index mappings for structured property changes.
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
   * Checks if this mapping strategy is enabled.
   *
   * @return true if enabled, false otherwise
   */
  boolean isEnabled();
}
