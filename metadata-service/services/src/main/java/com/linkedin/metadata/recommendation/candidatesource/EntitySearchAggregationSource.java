package com.linkedin.metadata.recommendation.candidatesource;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.ContentParams;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.SearchParams;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.utils.QueryUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Base class for search aggregation based candidate source (e.g. top platform, top tags, top terms)
 * Aggregates entities based on field value in the entity search index and gets the value with the
 * most documents
 */
@Slf4j
@RequiredArgsConstructor
public abstract class EntitySearchAggregationSource implements RecommendationSource {

  private final EntityService entityService;
  private final EntitySearchService entitySearchService;
  private final EntityRegistry entityRegistry;

  /** Field to aggregate on */
  protected abstract String getSearchFieldName();

  /** Max number of contents in module */
  protected abstract int getMaxContent();

  /** Whether the aggregate value is an urn */
  protected abstract boolean isValueUrn();

  /**
   * Filters the aggregated urn candidates down to the valid ones in a single batched lookup.
   * Subclasses that need aspect-level validation should override this and batch the aspect fetch
   * rather than checking one urn at a time.
   */
  protected Set<Urn> getValidCandidateUrns(
      @Nonnull OperationContext opContext, @Nonnull Set<Urn> candidateUrns) {
    return entityService.exists(opContext, candidateUrns, false);
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext,
      @Nullable RecommendationRequestContext requestContext,
      @Nullable Filter filter) {
    Map<String, Long> aggregationResult =
        entitySearchService.aggregateByValue(
            opContext,
            getEntityNames(entityRegistry),
            getSearchFieldName(),
            filter,
            getMaxContent());

    if (aggregationResult.isEmpty()) {
      return Collections.emptyList();
    }

    // If the aggregated values are not urn, simply get top k values with the most counts
    if (!isValueUrn()) {
      return getTopKValues(aggregationResult).stream()
          .map(entry -> buildRecommendationContent(entry.getKey(), entry.getValue()))
          .collect(Collectors.toList());
    }

    // If the aggregated values are urns, convert key into urns
    Map<Urn, Long> urnCounts =
        aggregationResult.entrySet().stream()
            .map(
                entry -> {
                  try {
                    Urn entityUrn = Urn.createFromString(entry.getKey());
                    return Optional.of(Pair.of(entityUrn, entry.getValue()));
                  } catch (URISyntaxException e) {
                    log.error("Invalid entity urn {}", entry.getKey(), e);
                    return Optional.<Pair<Urn, Long>>empty();
                  }
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    if (urnCounts.isEmpty()) {
      return Collections.emptyList();
    }

    // Validate all candidate urns in a single batched lookup, then take the top X with the most
    // documents. The candidate set is already bounded by the aggregation size (getMaxContent()).
    final Set<Urn> validUrns = getValidCandidateUrns(opContext, urnCounts.keySet());
    Map<Urn, Long> validUrnCounts =
        urnCounts.entrySet().stream()
            .filter(entry -> validUrns.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return getTopKValues(validUrnCounts).stream()
        .map(entry -> buildRecommendationContent(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  protected List<String> getEntityNames(EntityRegistry entityRegistry) {
    // By default, no list is applied which means searching across entities.
    return QueryUtils.getQueryByDefaultEntitySpecs(entityRegistry).stream()
        .map(EntitySpec::getName)
        .collect(Collectors.toList());
  }

  // Get top K entries with the most count from an already-validated candidate map
  private <T> List<Map.Entry<T, Long>> getTopKValues(Map<T, Long> countMap) {
    final PriorityQueue<Map.Entry<T, Long>> queue =
        new PriorityQueue<>(getMaxContent(), Map.Entry.comparingByValue(Comparator.naturalOrder()));
    for (Map.Entry<T, Long> entry : countMap.entrySet()) {
      if (queue.size() < getMaxContent()) {
        queue.add(entry);
      } else if (queue.size() > 0 && queue.peek().getValue() < entry.getValue()) {
        queue.poll();
        queue.add(entry);
      }
    }

    // Since priority queue polls in reverse order (nature of heaps), need to reverse order before
    // returning
    final LinkedList<Map.Entry<T, Long>> topK = new LinkedList<>();
    while (!queue.isEmpty()) {
      topK.addFirst(queue.poll());
    }
    return topK;
  }

  private <T> RecommendationContent buildRecommendationContent(T candidate, long count) {
    // Set filters for platform
    SearchParams searchParams =
        new SearchParams()
            .setQuery("")
            .setFilters(
                new CriterionArray(
                    ImmutableList.of(
                        buildCriterion(
                            getSearchFieldName(), Condition.EQUAL, candidate.toString()))));
    ContentParams contentParams = new ContentParams().setCount(count);
    RecommendationContent content = new RecommendationContent();
    if (candidate instanceof Urn) {
      content.setEntity((Urn) candidate);
    }
    return content
        .setValue(candidate.toString())
        .setParams(
            new RecommendationParams()
                .setSearchParams(searchParams)
                .setContentParams(contentParams));
  }
}
