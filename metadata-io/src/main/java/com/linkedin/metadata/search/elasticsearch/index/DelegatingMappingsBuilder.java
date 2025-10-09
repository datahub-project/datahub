package com.linkedin.metadata.search.elasticsearch.index;

import com.google.common.collect.Maps;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.index.entity.v2.LegacyMappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Delegating MappingsBuilder that forwards calls to the appropriate v2 or v3 implementation based
 * on the EntityIndexConfiguration flags. Both v2 and v3 can be enabled simultaneously.
 */
@Slf4j
public class DelegatingMappingsBuilder implements MappingsBuilder {

  private final List<MappingsBuilder> builders;
  private final boolean v2Enabled;
  private final boolean v3Enabled;

  public DelegatingMappingsBuilder(@Nonnull EntityIndexConfiguration entityIndexConfiguration) {
    this.v2Enabled =
        entityIndexConfiguration.getV2() != null && entityIndexConfiguration.getV2().isEnabled();
    this.v3Enabled =
        entityIndexConfiguration.getV3() != null && entityIndexConfiguration.getV3().isEnabled();

    this.builders = new ArrayList<>();

    // Initialize v2 builder if enabled
    if (v2Enabled) {
      this.builders.add(new LegacyMappingsBuilder(entityIndexConfiguration));
    }

    // Initialize v3 builder if enabled
    if (v3Enabled) {
      try {
        this.builders.add(new MultiEntityMappingsBuilder(entityIndexConfiguration));
      } catch (IOException e) {
        log.error("Failed to initialize MultiEntityMappingsBuilder", e);
        throw new RuntimeException("Failed to initialize MultiEntityMappingsBuilder", e);
      }
    }

    // If no builders are available, use NoOpMappingsBuilder
    if (this.builders.isEmpty()) {
      log.warn("Neither v2 nor v3 entity index is enabled. Using NoOpMappingsBuilder.");
      this.builders.add(new NoOpMappingsBuilder());
    }
  }

  @Override
  public Collection<IndexMapping> getIndexMappings(
      @Nonnull OperationContext opContext,
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> structuredProperties) {
    if (builders.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the first builder's result as the reference
    MappingsBuilder firstBuilder = builders.get(0);
    Collection<IndexMapping> referenceMappings;
    try {
      referenceMappings = firstBuilder.getIndexMappings(opContext, structuredProperties);
      log.debug(
          "Reference mappings from {}: {} mappings",
          firstBuilder.getClass().getSimpleName(),
          referenceMappings.size());
    } catch (Exception e) {
      log.error(
          "Failed to get reference mappings from {}", firstBuilder.getClass().getSimpleName(), e);
      throw new RuntimeException("Failed to get reference mappings", e);
    }

    // Start with reference mappings as the base
    Map<String, IndexMapping> mergedMappings = new HashMap<>();
    referenceMappings.forEach(mapping -> mergedMappings.put(mapping.getIndexName(), mapping));

    // Validate and merge mappings from all other builders
    for (int i = 1; i < builders.size(); i++) {
      MappingsBuilder builder = builders.get(i);
      try {
        Collection<IndexMapping> mappings =
            builder.getIndexMappings(opContext, structuredProperties);

        // Validate consistency for overlapping indices
        if (!mappingsEqual(referenceMappings, mappings)) {
          log.error(
              "Inconsistent mappings detected between {} and {}",
              firstBuilder.getClass().getSimpleName(),
              builder.getClass().getSimpleName());
          log.error("Reference mappings: {}", referenceMappings);
          log.error("Conflicting mappings: {}", mappings);
          throw new IllegalStateException(
              String.format(
                  "Inconsistent mappings between %s and %s. This indicates an illegal configuration state.",
                  firstBuilder.getClass().getSimpleName(), builder.getClass().getSimpleName()));
        }

        // Add new mappings that don't overlap with existing ones
        for (IndexMapping mapping : mappings) {
          String indexName = mapping.getIndexName();
          if (!mergedMappings.containsKey(indexName)) {
            mergedMappings.put(indexName, mapping);
            log.debug("Added new mapping for index: {}", indexName);
          }
        }

        log.debug(
            "Validated and merged mappings from {}: {} mappings",
            builder.getClass().getSimpleName(),
            mappings.size());
      } catch (Exception e) {
        log.error("Failed to validate mappings from {}", builder.getClass().getSimpleName(), e);
        throw new RuntimeException("Failed to validate mappings", e);
      }
    }

    return mergedMappings.values();
  }

  @Override
  public Collection<IndexMapping> getMappings(@Nonnull OperationContext opContext) {
    return getIndexMappings(opContext, null);
  }

  @Override
  public Collection<IndexMapping> getIndexMappingsWithNewStructuredProperty(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull StructuredPropertyDefinition property) {
    if (builders.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the first builder's result as the reference
    MappingsBuilder firstBuilder = builders.get(0);
    Collection<IndexMapping> referenceMappings;
    try {
      referenceMappings =
          firstBuilder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);
      log.debug(
          "Reference mappings from {} for new structured property: {} mappings",
          firstBuilder.getClass().getSimpleName(),
          referenceMappings.size());
    } catch (Exception e) {
      log.error(
          "Failed to get reference mappings from {} for new structured property",
          firstBuilder.getClass().getSimpleName(),
          e);
      throw new RuntimeException("Failed to get reference mappings for new structured property", e);
    }

    // Start with reference mappings as the base
    Map<String, IndexMapping> mergedMappings = new HashMap<>();
    referenceMappings.forEach(mapping -> mergedMappings.put(mapping.getIndexName(), mapping));

    // Validate and merge mappings from all other builders
    for (int i = 1; i < builders.size(); i++) {
      MappingsBuilder builder = builders.get(i);
      try {
        Collection<IndexMapping> mappings =
            builder.getIndexMappingsWithNewStructuredProperty(opContext, urn, property);

        // Validate consistency for overlapping indices
        if (!mappingsEqual(referenceMappings, mappings)) {
          log.error(
              "Inconsistent mappings detected between {} and {} for new structured property",
              firstBuilder.getClass().getSimpleName(),
              builder.getClass().getSimpleName());
          log.error("Reference mappings: {}", referenceMappings);
          log.error("Conflicting mappings: {}", mappings);
          throw new IllegalStateException(
              String.format(
                  "Inconsistent mappings between %s and %s for new structured property. This indicates an illegal configuration state.",
                  firstBuilder.getClass().getSimpleName(), builder.getClass().getSimpleName()));
        }

        // Add new mappings that don't overlap with existing ones
        for (IndexMapping mapping : mappings) {
          String indexName = mapping.getIndexName();
          if (!mergedMappings.containsKey(indexName)) {
            mergedMappings.put(indexName, mapping);
            log.debug("Added new mapping for index: {} for new structured property", indexName);
          }
        }

        log.debug(
            "Validated and merged mappings from {} for new structured property: {} mappings",
            builder.getClass().getSimpleName(),
            mappings.size());
      } catch (Exception e) {
        log.error(
            "Failed to validate mappings from {} for new structured property",
            builder.getClass().getSimpleName(),
            e);
        throw new RuntimeException("Failed to validate mappings for new structured property", e);
      }
    }

    return mergedMappings.values();
  }

  @Override
  public Map<String, Object> getMappingsForStructuredProperty(
      Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    if (builders.isEmpty()) {
      return Collections.emptyMap();
    }

    // Get the first builder's result as the reference
    MappingsBuilder firstBuilder = builders.get(0);
    Map<String, Object> referenceMappings;
    try {
      referenceMappings = firstBuilder.getMappingsForStructuredProperty(properties);
      log.debug(
          "Reference structured property mappings from {}: {} entries",
          firstBuilder.getClass().getSimpleName(),
          referenceMappings.size());
    } catch (Exception e) {
      log.error(
          "Failed to get reference structured property mappings from {}",
          firstBuilder.getClass().getSimpleName(),
          e);
      throw new RuntimeException("Failed to get reference structured property mappings", e);
    }

    // Validate that all other builders return consistent results
    for (int i = 1; i < builders.size(); i++) {
      MappingsBuilder builder = builders.get(i);
      try {
        Map<String, Object> mappings = builder.getMappingsForStructuredProperty(properties);
        if (!mapsEqual(referenceMappings, mappings)) {
          log.error(
              "Inconsistent structured property mappings detected between {} and {}",
              firstBuilder.getClass().getSimpleName(),
              builder.getClass().getSimpleName());
          log.error("Reference mappings: {}", referenceMappings);
          log.error("Conflicting mappings: {}", mappings);
          throw new IllegalStateException(
              String.format(
                  "Inconsistent structured property mappings between %s and %s. This indicates an illegal configuration state.",
                  firstBuilder.getClass().getSimpleName(), builder.getClass().getSimpleName()));
        }
        log.debug(
            "Validated structured property mappings from {}: {} entries",
            builder.getClass().getSimpleName(),
            mappings.size());
      } catch (Exception e) {
        log.error(
            "Failed to validate structured property mappings from {}",
            builder.getClass().getSimpleName(),
            e);
        throw new RuntimeException("Failed to validate structured property mappings", e);
      }
    }

    return referenceMappings;
  }

  @Override
  public Map<String, Object> getMappings(
      @Nonnull EntityRegistry entityRegistry, @Nonnull EntitySpec entitySpec) {
    if (builders.isEmpty()) {
      return Collections.emptyMap();
    }

    // TODO: Consider how to handle multiple builders for single entity spec mappings
    // For now, just return the first builder's result
    MappingsBuilder firstBuilder = builders.get(0);
    return firstBuilder.getMappings(entityRegistry, entitySpec);
  }

  /**
   * Compares two collections of IndexMapping for equality. Two collections are considered equal if
   * they have the same mappings for indices that exist in both collections. It's fine if one
   * collection has additional indices that the other doesn't.
   */
  private boolean mappingsEqual(
      Collection<IndexMapping> mappings1, Collection<IndexMapping> mappings2) {
    // Convert to maps for easier comparison
    Map<String, IndexMapping> map1 =
        mappings1.stream()
            .collect(Collectors.toMap(IndexMapping::getIndexName, mapping -> mapping));
    Map<String, IndexMapping> map2 =
        mappings2.stream()
            .collect(Collectors.toMap(IndexMapping::getIndexName, mapping -> mapping));

    // Find common indices
    Set<String> commonIndices = new HashSet<>(map1.keySet());
    commonIndices.retainAll(map2.keySet());

    // Compare mappings for common indices only
    for (String indexName : commonIndices) {
      IndexMapping mapping1 = map1.get(indexName);
      IndexMapping mapping2 = map2.get(indexName);

      if (!mapping1.getMappings().equals(mapping2.getMappings())) {
        return false;
      }
    }

    return true;
  }

  /**
   * Compares two maps for equality using Google Guava's Maps.difference(). This performs deep
   * comparison of nested maps and collections.
   */
  private boolean mapsEqual(Map<String, Object> map1, Map<String, Object> map2) {
    if (map1 == map2) {
      return true;
    }
    if (map1 == null || map2 == null) {
      return false;
    }

    return Maps.difference(map1, map2).areEqual();
  }
}
