package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
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
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.cache.CacheManager;


@Slf4j
public class TopTagsCandidateSource implements RecommendationCandidateSource {
  private final EntitySearchService _entitySearchService;
  private final NonEmptyEntitiesCache _nonEmptyEntitiesCache;

  private static final String TAGS = "tags";
  private static final int MAX_CONTENT = 10;

  public TopTagsCandidateSource(EntitySearchService entitySearchService, EntityRegistry entityRegistry,
      CacheManager cacheManager) {
    _entitySearchService = entitySearchService;
    _nonEmptyEntitiesCache = new NonEmptyEntitiesCache(entityRegistry, entitySearchService, cacheManager);
  }

  @Override
  public String getTitle() {
    return "Top Tags";
  }

  @Override
  public String getModuleId() {
    return "TopTags";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.TAG_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME
        || requestContext.getScenario() == ScenarioType.SEARCH_RESULTS;
  }

  @Override
  public List<RecommendationContent> getCandidates(@Nonnull Urn userUrn,
      @Nullable RecommendationRequestContext requestContext) {
    // Fetch number of documents per platform for each entity type
    List<Map<String, Long>> resultPerEntity =
        ConcurrencyUtils.transformAndCollectAsync(_nonEmptyEntitiesCache.getNonEmptyEntities(),
            entity -> _entitySearchService.aggregateByValue(entity, TAGS, null, 100));

    // Merge the aggregated result into one
    Map<String, Long> mergedResult =
        resultPerEntity.stream().reduce(this::mergeAggregation).orElse(Collections.emptyMap());

    // Convert key into urns
    Map<Urn, Long> tagCounts = mergedResult.entrySet().stream().map(entry -> {
      try {
        Urn tagUrn = Urn.createFromString(entry.getKey());
        return Optional.of(Pair.of(tagUrn, entry.getValue()));
      } catch (URISyntaxException e) {
        log.error("Invalid tag urn {}", entry.getKey(), e);
        return Optional.<Pair<Urn, Long>>empty();
      }
    }).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    if (tagCounts.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the top X valid platforms (ones with logo) with the most number of documents
    PriorityQueue<Map.Entry<Urn, Long>> queue =
        new PriorityQueue<>(MAX_CONTENT, Map.Entry.comparingByValue(Comparator.reverseOrder()));
    for (Map.Entry<Urn, Long> entry : tagCounts.entrySet()) {
      if (queue.size() < MAX_CONTENT) {
        queue.add(entry);
      } else if (queue.peek().getValue() < entry.getValue()) {
        queue.poll();
        queue.add(entry);
      }
    }

    return queue.stream()
        .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder()))
        .map(entry -> buildRecommendationContent(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  private Map<String, Long> mergeAggregation(Map<String, Long> first, Map<String, Long> second) {
    return Stream.concat(first.entrySet().stream(), second.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
  }

  private RecommendationContent buildRecommendationContent(Urn tagUrn, long count) {
    // Set filters for platform
    SearchParams searchParams = new SearchParams().setQuery("")
        .setFilters(
            new CriterionArray(ImmutableList.of(new Criterion().setField(TAGS).setValue(tagUrn.toString()))));
    ContentParams contentParams = new ContentParams().setCount(count);
    return new RecommendationContent().setValue(tagUrn.toString())
        .setEntity(tagUrn)
        .setParams(new RecommendationParams().setSearchParams(searchParams).setContentParams(contentParams));
  }
}
