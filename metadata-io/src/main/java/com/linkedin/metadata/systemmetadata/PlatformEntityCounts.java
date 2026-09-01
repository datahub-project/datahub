package com.linkedin.metadata.systemmetadata;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.filter.Filter;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Inventory counts by {@code platform} from entity search indexes (not system-metadata). Thin
 * orchestration over a terms aggregation with a {@link #NO_PLATFORM} missing bucket — not a
 * parallel count domain service.
 */
@Slf4j
public class PlatformEntityCounts {

  /** Sentinel for documents with no platform value (matches semantic coverage). */
  public static final String NO_PLATFORM = "NO_PLATFORM";

  private static final String PLATFORM_FIELD = "platform";
  private static final String PLATFORM_AGG_FIELD = "platform.keyword";
  private static final String PLATFORM_AGG_NAME = "by_platform";
  private static final String ACTIVE_AGG_NAME = "active";
  private static final String SOFT_DELETED_AGG_NAME = "soft_deleted";
  private static final int MAX_PLATFORM_BUCKETS = 500;
  private static final String DATA_PLATFORM_INSTANCE_ASPECT = "dataPlatformInstance";

  private final SearchClientShim<?> searchClient;
  private final EntityRegistry entityRegistry;
  private final int maxEntityTypes;

  public PlatformEntityCounts(
      @Nonnull SearchClientShim<?> searchClient,
      @Nonnull EntityRegistry entityRegistry,
      int maxEntityTypes) {
    this.searchClient = Objects.requireNonNull(searchClient, "searchClient");
    this.entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry");
    this.maxEntityTypes = maxEntityTypes;
  }

  @Nonnull
  public PlatformEntityCountResult getCountsByPlatform(
      @Nonnull OperationContext opContext, @Nullable List<String> entityTypes) {
    List<String> resolved = resolveEntityTypes(entityTypes);
    List<PlatformEntityCountEntry> entries = new ArrayList<>();
    for (String entityType : resolved) {
      if (!hasPlatformSearchField(entityType)) {
        log.debug("Skipping entity type {} — no searchable platform field", entityType);
        continue;
      }
      try {
        entries.addAll(countEntityTypeByPlatform(opContext, entityType));
      } catch (IOException e) {
        log.warn("Platform count failed for entity type {}; skipping", entityType, e);
      }
    }
    return PlatformEntityCountResult.builder()
        .counts(entries)
        .requestedTypes(resolved)
        .computedAt(Instant.now())
        .build();
  }

  @Nonnull
  private List<PlatformEntityCountEntry> countEntityTypeByPlatform(
      @Nonnull OperationContext opContext, @Nonnull String entityType) throws IOException {
    String indexName =
        opContext.getSearchContext().getIndexConvention().getEntityIndexName(opContext, entityType);

    SearchSourceBuilder source =
        new SearchSourceBuilder()
            .query(QueryBuilders.matchAllQuery())
            .size(0)
            .trackTotalHits(false)
            .aggregation(
                AggregationBuilders.terms(PLATFORM_AGG_NAME)
                    .field(PLATFORM_AGG_FIELD)
                    .missing(NO_PLATFORM)
                    .size(MAX_PLATFORM_BUCKETS)
                    .subAggregation(
                        AggregationBuilders.filter(
                            ACTIVE_AGG_NAME,
                            QueryBuilders.boolQuery()
                                .mustNot(QueryBuilders.termQuery(ESUtils.REMOVED, true))))
                    .subAggregation(
                        AggregationBuilders.filter(
                            SOFT_DELETED_AGG_NAME,
                            QueryBuilders.termQuery(ESUtils.REMOVED, true))));

    SearchRequest request = new SearchRequest(indexName).source(source);
    SearchResponse response;
    try {
      response = searchClient.search(opContext, request, RequestOptions.DEFAULT);
    } catch (OpenSearchStatusException e) {
      if (e.status() == RestStatus.NOT_FOUND) {
        log.debug("Entity index {} not found; returning empty platform counts", indexName);
        return List.of();
      }
      throw e;
    }

    Terms platformTerms = response.getAggregations().get(PLATFORM_AGG_NAME);
    if (platformTerms == null) {
      return List.of();
    }
    long otherDocCount = platformTerms.getSumOfOtherDocCounts();
    if (otherDocCount > 0) {
      log.warn(
          "Platform terms aggregation for {} truncated {} docs outside top {} buckets",
          entityType,
          otherDocCount,
          MAX_PLATFORM_BUCKETS);
    }

    List<PlatformEntityCountEntry> entries = new ArrayList<>();
    for (Terms.Bucket bucket : platformTerms.getBuckets()) {
      Filter activeFilter = bucket.getAggregations().get(ACTIVE_AGG_NAME);
      Filter softFilter = bucket.getAggregations().get(SOFT_DELETED_AGG_NAME);
      long active = activeFilter != null ? activeFilter.getDocCount() : 0L;
      long soft = softFilter != null ? softFilter.getDocCount() : 0L;
      if (active == 0L && soft == 0L) {
        continue;
      }
      entries.add(
          PlatformEntityCountEntry.builder()
              .entityType(entityType)
              .platform(normalizePlatform(bucket.getKeyAsString()))
              .activeCount(active)
              .softDeletedCount(soft)
              .build());
    }
    return entries;
  }

  @Nonnull
  static String normalizePlatform(@Nonnull String raw) {
    if (NO_PLATFORM.equals(raw) || raw.isBlank()) {
      return NO_PLATFORM;
    }
    String trimmed = raw.trim();
    if (trimmed.startsWith("urn:li:dataPlatform:")) {
      try {
        return Urn.createFromString(trimmed).getId();
      } catch (URISyntaxException e) {
        int colon = trimmed.lastIndexOf(':');
        if (colon >= 0 && colon < trimmed.length() - 1) {
          return trimmed.substring(colon + 1);
        }
      }
    }
    return trimmed;
  }

  boolean hasPlatformSearchField(@Nonnull String entityType) {
    EntitySpec spec = entityRegistry.getEntitySpec(entityType);
    if (spec == null) {
      return false;
    }
    if (Boolean.TRUE.equals(spec.hasAspect(DATA_PLATFORM_INSTANCE_ASPECT))) {
      return true;
    }
    for (SearchableFieldSpec fieldSpec : spec.getSearchableFieldSpecs()) {
      if (PLATFORM_FIELD.equals(fieldSpec.getSearchableAnnotation().getFieldName())) {
        return true;
      }
    }
    return false;
  }

  @Nonnull
  private List<String> resolveEntityTypes(@Nullable List<String> entityTypes) {
    if (entityTypes == null || entityTypes.isEmpty()) {
      return entityRegistry.getEntitySpecs().keySet().stream()
          .sorted(Comparator.naturalOrder())
          .collect(Collectors.toList());
    }
    List<String> normalized =
        entityTypes.stream()
            .map(t -> t.toLowerCase(Locale.ROOT))
            .distinct()
            .sorted()
            .collect(Collectors.toList());
    if (normalized.size() > maxEntityTypes) {
      throw new IllegalArgumentException(
          "Requested entity type count "
              + normalized.size()
              + " exceeds maximum "
              + maxEntityTypes);
    }
    for (String entityType : normalized) {
      if (!entityRegistry.getEntitySpecs().containsKey(entityType)) {
        throw new IllegalArgumentException("Unknown entity type: " + entityType);
      }
    }
    return normalized;
  }
}
