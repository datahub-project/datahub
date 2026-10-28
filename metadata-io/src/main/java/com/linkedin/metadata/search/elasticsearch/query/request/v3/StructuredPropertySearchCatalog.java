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
import com.linkedin.data.template.GetMode;
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
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.StructuredPropertyUrnIdentity;
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

  /**
   * One definition snapshot per catalog. Search result caches key the query inputs that change a
   * response; this list does not. The retriever only loads it.
   */
  private enum SnapshotKey {
    DEFINITIONS
  }

  private static final ThreadLocal<Boolean> LOADING = ThreadLocal.withInitial(() -> Boolean.FALSE);

  private final StructuredPropertyQueryConfiguration config;
  private final Cache<SnapshotKey, List<IndexedProperty>> cache;

  public StructuredPropertySearchCatalog(@Nullable StructuredPropertyQueryConfiguration config) {
    if (config == null) {
      config =
          StructuredPropertyQueryConfiguration.builder()
              .cacheTtlSeconds(300)
              .defaultTextWeight(0.8f)
              .stemmedFactor(0.5f)
              .pageSize(500)
              .maxProperties(1000)
              .build();
    }
    if (config.getPageSize() < 1
        || config.getMaxProperties() < 1
        || config.getCacheTtlSeconds() < 1) {
      throw new IllegalArgumentException(
          "elasticsearch.search.structuredProperties requires a positive page size, property limit, and cache TTL");
    }
    this.config = config;
    this.cache = newCache(config);
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
    return all(opContext).stream().filter(property -> property.appliesTo(entitySpecs)).toList();
  }

  /** Stable request-cache identity for the currently loaded definitions. */
  @Nonnull
  public String catalogStamp(@Nullable OperationContext opContext) {
    if (opContext == null) {
      return "0";
    }
    return Integer.toHexString(all(opContext).hashCode());
  }

  /**
   * Loading the snapshot issues a search, and building that search reads this same snapshot
   * (template binding stamps the cache identity, and default filters resolve lifecycle stages
   * through another search). The load therefore runs outside the cache. A nested read returns no
   * properties instead of starting another load; publishing the finished list is a separate write.
   */
  @Nonnull
  private List<IndexedProperty> all(@Nonnull OperationContext opContext) {
    List<IndexedProperty> cached = cache.getIfPresent(SnapshotKey.DEFINITIONS);
    if (cached != null) {
      return cached;
    }
    if (Boolean.TRUE.equals(LOADING.get())) {
      return List.of();
    }
    LOADING.set(Boolean.TRUE);
    try {
      List<IndexedProperty> loaded =
          load(opContext, opContext.getRetrieverContext().getSearchRetriever());
      cache.put(SnapshotKey.DEFINITIONS, loaded);
      return loaded;
    } finally {
      LOADING.remove();
    }
  }

  @Nonnull
  private static Cache<SnapshotKey, List<IndexedProperty>> newCache(
      @Nonnull StructuredPropertyQueryConfiguration config) {
    return CacheBuilder.newBuilder()
        .maximumSize(1)
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
    String fieldPath =
        V3SearchFields.STRUCTURED_PROPERTIES
            + "."
            + toElasticsearchFieldName(propertyUrn, definition);
    String fullTextPath =
        logicalValueType == LogicalValueType.URN
            ? fieldPath + StructuredPropertyUrnIdentity.FIELD_SUFFIX
            : fieldPath;
    return Optional.of(
        new IndexedProperty(
            propertyUrn,
            definition.getQualifiedName(),
            definition.hasDisplayName() ? definition.getDisplayName() : null,
            definition.getVersion(GetMode.NULL),
            logicalValueType,
            fieldPath,
            definition.getEntityTypes(),
            config.isQueryByDefault(),
            textWeight,
            stemmedWeight(config, textWeight),
            copyTo,
            fullTextPath));
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
   * field configuration uses. {@code fullTextPath} is that field for a string or rich text value,
   * and the identity sibling for a URN. Weights are before the partial factor.
   */
  public record IndexedProperty(
      @Nonnull Urn propertyUrn,
      @Nonnull String qualifiedName,
      @Nullable String displayName,
      @Nullable String version,
      @Nonnull LogicalValueType valueType,
      @Nonnull String fieldPath,
      @Nonnull Collection<Urn> entityTypes,
      boolean queryByDefault,
      float textWeight,
      float stemmedWeight,
      @Nonnull List<String> copyTo,
      @Nonnull String fullTextPath) {

    public boolean appliesTo(@Nonnull Collection<EntitySpec> entitySpecs) {
      return entitySpecs.stream()
          .anyMatch(
              spec ->
                  entityTypes.stream()
                      .anyMatch(entityType -> entityTypeMatches(entityType, spec.getName())));
    }
  }
}
