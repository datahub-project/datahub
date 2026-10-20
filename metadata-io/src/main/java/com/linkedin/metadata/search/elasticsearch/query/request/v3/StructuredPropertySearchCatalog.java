package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_ENTITY_NAME;
import static com.linkedin.metadata.models.StructuredPropertyUtils.entityTypeMatches;
import static com.linkedin.metadata.models.StructuredPropertyUtils.getLogicalValueType;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.config.search.StructuredPropertyQueryConfiguration;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.structured.StructuredPropertyDefinition;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Structured properties Search V3 full-text reads. Active property URNs come from search, which
 * excludes soft-deleted entities. Definitions come from the cached entity client.
 */
@Slf4j
public final class StructuredPropertySearchCatalog {

  private static final ConcurrentHashMap<
          StructuredPropertyQueryConfiguration, Cache<SearchRetriever, List<IndexedProperty>>>
      CACHES = new ConcurrentHashMap<>();

  private final StructuredPropertyQueryConfiguration config;
  private final Cache<SearchRetriever, List<IndexedProperty>> cache;

  public StructuredPropertySearchCatalog(@Nonnull StructuredPropertyQueryConfiguration config) {
    if (config.getPageSize() < 1
        || config.getMaxProperties() < 1
        || config.getCacheTtlSeconds() < 1) {
      throw new IllegalArgumentException(
          "elasticsearch.search.structuredProperties requires a positive page size, property limit, and cache TTL");
    }
    this.config = config;
    this.cache = CACHES.computeIfAbsent(config, StructuredPropertySearchCatalog::newCache);
  }

  /**
   * Opted-in properties that apply to at least one of {@code entitySpecs}. A property mapped on
   * only some indices of a multi-index search is still returned; the query is lenient on the
   * others.
   */
  @Nonnull
  public List<IndexedProperty> indexed(
      @Nullable OperationContext opContext, @Nonnull Collection<EntitySpec> entitySpecs) {
    if (opContext == null
        || entitySpecs.stream()
            .noneMatch(entitySpec -> entitySpec.hasAspect(STRUCTURED_PROPERTIES_ASPECT_NAME))) {
      return List.of();
    }
    SearchRetriever searchRetriever = opContext.getRetrieverContext().getSearchRetriever();
    List<IndexedProperty> all =
        cache.asMap().computeIfAbsent(searchRetriever, ignored -> load(opContext, searchRetriever));
    return all.stream().filter(property -> property.appliesTo(entitySpecs)).toList();
  }

  @Nonnull
  private static Cache<SearchRetriever, List<IndexedProperty>> newCache(
      @Nonnull StructuredPropertyQueryConfiguration config) {
    return CacheBuilder.newBuilder()
        .maximumSize(config.getMaxProperties())
        .expireAfterWrite(config.getCacheTtlSeconds(), TimeUnit.SECONDS)
        .build();
  }

  @Nonnull
  private List<IndexedProperty> load(
      @Nonnull OperationContext opContext, @Nonnull SearchRetriever searchRetriever) {
    SearchFlags flags = new SearchFlags(new DataMap(SearchRetriever.RETRIEVER_SEARCH_FLAGS.data()));
    SortCriterion urnSort = new SortCriterion().setField("urn").setOrder(SortOrder.ASCENDING);
    CachingAspectRetriever aspectRetriever =
        opContext.getRetrieverContext().getCachingAspectRetriever();

    List<IndexedProperty> properties = new ArrayList<>();
    String scrollId = null;
    while (properties.size() < config.getMaxProperties()) {
      int pageSize = Math.min(config.getPageSize(), config.getMaxProperties() - properties.size());
      ScrollResult page =
          searchRetriever.scroll(
              List.of(STRUCTURED_PROPERTY_ENTITY_NAME),
              null,
              scrollId,
              pageSize,
              List.of(urnSort),
              flags);
      if (page.getEntities() == null || page.getEntities().isEmpty()) {
        break;
      }
      Set<Urn> urns = new LinkedHashSet<>();
      for (SearchEntity entity : page.getEntities()) {
        if (urns.size() >= pageSize) {
          break;
        }
        urns.add(entity.getEntity());
      }
      Map<Urn, Map<String, Aspect>> aspects =
          aspectRetriever.getLatestAspectObjects(
              opContext, urns, Set.of(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME));
      for (Urn urn : urns) {
        if (properties.size() >= config.getMaxProperties()) {
          break;
        }
        // One unreadable definition must not fail the query. A thrown load is not cached, so the
        // failure would repeat on every search.
        try {
          Aspect definitionAspect =
              aspects.getOrDefault(urn, Map.of()).get(STRUCTURED_PROPERTY_DEFINITION_ASPECT_NAME);
          if (definitionAspect == null || definitionAspect.data() == null) {
            continue;
          }
          indexedProperty(
                  urn, new StructuredPropertyDefinition(new DataMap(definitionAspect.data())))
              .ifPresent(properties::add);
        } catch (RuntimeException e) {
          log.warn("Skipping structured property {} with an unreadable definition", urn, e);
        }
      }
      scrollId = page.getScrollId();
      if (scrollId == null) {
        break;
      }
    }
    return List.copyOf(properties);
  }

  @Nonnull
  private Optional<IndexedProperty> indexedProperty(
      @Nonnull Urn propertyUrn, @Nonnull StructuredPropertyDefinition definition) {
    if (!definition.hasSearchConfiguration()
        || definition.getEntityTypes() == null
        || definition.getEntityTypes().isEmpty()) {
      return Optional.empty();
    }
    LogicalValueType logicalValueType = getLogicalValueType(definition.getValueType());
    if (logicalValueType != LogicalValueType.STRING
        && logicalValueType != LogicalValueType.RICH_TEXT
        && logicalValueType != LogicalValueType.URN) {
      return Optional.empty();
    }
    DataHubSearchConfig config = definition.getSearchConfiguration();
    float textWeight = textWeight(config);
    List<String> copyTo =
        config.hasCopyTo()
            ? config.getCopyTo().stream().filter(name -> name != null && !name.isBlank()).toList()
            : List.of();
    return Optional.of(
        new IndexedProperty(
            V3SearchFields.STRUCTURED_PROPERTIES
                + "."
                + toElasticsearchFieldName(propertyUrn, definition),
            definition.getEntityTypes(),
            config.isQueryByDefault(),
            textWeight,
            stemmedWeight(config, textWeight),
            copyTo));
  }

  private float textWeight(@Nonnull DataHubSearchConfig config) {
    if (config.data().containsKey("boostScore")) {
      return config.getBoostScore().floatValue();
    }
    return this.config.getDefaultTextWeight();
  }

  private float stemmedWeight(@Nonnull DataHubSearchConfig config, final float textWeight) {
    if (config.hasSubfieldBoostScores()
        && config.getSubfieldBoostScores().containsKey(V3SearchFields.STEMMED)) {
      return config.getSubfieldBoostScores().get(V3SearchFields.STEMMED).floatValue();
    }
    return textWeight * this.config.getStemmedFactor();
  }

  /**
   * One opted-in property. {@code fieldPath} is {@code structuredProperties.<field>}, the name a
   * field configuration uses. Weights are before the partial factor.
   */
  public record IndexedProperty(
      @Nonnull String fieldPath,
      @Nonnull Collection<Urn> entityTypes,
      boolean queryByDefault,
      float textWeight,
      float stemmedWeight,
      @Nonnull List<String> copyTo) {

    public boolean appliesTo(@Nonnull Collection<EntitySpec> entitySpecs) {
      return entitySpecs.stream()
          .anyMatch(
              spec ->
                  entityTypes.stream()
                      .anyMatch(entityType -> entityTypeMatches(entityType, spec.getName())));
    }
  }
}
