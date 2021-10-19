package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.recommendation.ContentParams;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.recommendation.SearchParams;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.cache.NonEmptyEntitiesCache;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;


@Slf4j
public class TopPlatformsCandidateSource implements RecommendationCandidateSource {
  private final EntitySearchService _entitySearchService;
  private final NonEmptyEntitiesCache _nonEmptyEntitiesCache;

  private static final String PLATFORM = "platform";
  private static final int MAX_CONTENT = 5;

  public TopPlatformsCandidateSource(EntitySearchService entitySearchService, EntityRegistry entityRegistry,
      CacheManager cacheManager) {
    _entitySearchService = entitySearchService;
    _nonEmptyEntitiesCache = new NonEmptyEntitiesCache(entityRegistry, entitySearchService, cacheManager);
  }

  @Override
  public String getTitle() {
    return "Top Platforms";
  }

  @Override
  public String getModuleId() {
    return "TopPlatforms";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.PLATFORM_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME;
  }

  @Override
  public List<RecommendationContent> getCandidates(@Nonnull Urn userUrn,
      @Nullable RecommendationRequestContext requestContext) {
    // Fetch number of documents per platform for each entity type
    List<Map<String, Long>> resultPerEntity =
        ConcurrencyUtils.transformAndCollectAsync(_nonEmptyEntitiesCache.getNonEmptyEntities(),
            entity -> _entitySearchService.aggregateByValue(entity, PLATFORM, null, 100));

    // Merge the aggregated result into one
    Map<String, Long> mergedResult =
        resultPerEntity.stream().reduce(this::mergeAggregation).orElse(Collections.emptyMap());
    if (mergedResult.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the top 5 platforms with the most number of documents
    PriorityQueue<Map.Entry<String, Long>> queue =
        new PriorityQueue<>(MAX_CONTENT, Map.Entry.comparingByValue(Comparator.naturalOrder()));
    for (Map.Entry<String, Long> entry : mergedResult.entrySet()) {
      queue.add(entry);
      if (queue.size() > MAX_CONTENT) {
        queue.poll();
      }
    }

    // Build recommendation
    return queue.stream()
        .map(entry -> buildRecommendationContent(entry.getKey(), entry.getValue()))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());
  }

  private Map<String, Long> mergeAggregation(Map<String, Long> first, Map<String, Long> second) {
    return Stream.concat(first.entrySet().stream(), second.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
  }

  private Optional<RecommendationContent> buildRecommendationContent(String platformUrn, long count) {
    // Set filters for platform
    SearchParams searchParams = new SearchParams().setQuery("")
        .setFilters(new CriterionArray(ImmutableList.of(new Criterion().setField(PLATFORM).setValue(platformUrn))));
    ContentParams contentParams = new ContentParams().setCount(count);
    try {
      return Optional.of(new RecommendationContent().setValue(platformUrn)
          .setEntity(Urn.createFromString(platformUrn))
          .setParams(new RecommendationParams().setSearchParams(searchParams).setContentParams(contentParams)));
    } catch (URISyntaxException e) {
      log.error("Error decoding platform URN: {}", platformUrn, e);
      return Optional.empty();
    }
  }
}
