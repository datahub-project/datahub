package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.recommendation.ContentParams;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.SearchParams;
import com.linkedin.metadata.search.EntitySearchService;
import io.opentelemetry.extension.annotations.WithSpan;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
  private final EntitySearchService _entitySearchService;

  /** Field to aggregate on */
  protected abstract String getSearchFieldName();

  /** Max number of contents in module */
  protected abstract int getMaxContent();

  /** Whether the aggregate value is an urn */
  protected abstract boolean isValueUrn();

  /** Whether the urn candidate is valid */
  protected boolean isValidCandidateUrn(Urn urn) {
    return true;
  }

  /** Whether the string candidate is valid */
  protected boolean isValidCandidateValue(String candidateValue) {
    return true;
  }

  /** Whether the candidate is valid Calls different functions if candidate is an Urn */
  protected <T> boolean isValidCandidate(T candidate) {
    if (candidate instanceof Urn) {
      return isValidCandidateUrn((Urn) candidate);
    }
    return isValidCandidateValue(candidate.toString());
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(
      @Nonnull Urn userUrn, @Nullable RecommendationRequestContext requestContext) {
    Map<String, Long> aggregationResult =
        _entitySearchService.aggregateByValue(
            getEntityNames(), getSearchFieldName(), null, getMaxContent());

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
                    Urn tagUrn = Urn.createFromString(entry.getKey());
                    return Optional.of(Pair.of(tagUrn, entry.getValue()));
                  } catch (URISyntaxException e) {
                    log.error("Invalid tag urn {}", entry.getKey(), e);
                    return Optional.<Pair<Urn, Long>>empty();
                  }
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

    if (urnCounts.isEmpty()) {
      return Collections.emptyList();
    }

    // Get the top X valid platforms (ones with logo) with the most number of documents
    return getTopKValues(urnCounts).stream()
        .map(entry -> buildRecommendationContent(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  protected List<String> getEntityNames() {
    // By default, no list is applied which means searching across entities.
    return null;
  }

  // Get top K entries with the most count
  private <T> List<Map.Entry<T, Long>> getTopKValues(Map<T, Long> countMap) {
    final PriorityQueue<Map.Entry<T, Long>> queue =
        new PriorityQueue<>(getMaxContent(), Map.Entry.comparingByValue(Comparator.naturalOrder()));
    for (Map.Entry<T, Long> entry : countMap.entrySet()) {
      if (queue.size() < getMaxContent() && isValidCandidate(entry.getKey())) {
        queue.add(entry);
      } else if (queue.size() > 0
          && queue.peek().getValue() < entry.getValue()
          && isValidCandidate(entry.getKey())) {
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

  private Map<String, Long> mergeAggregation(Map<String, Long> first, Map<String, Long> second) {
    return Stream.concat(first.entrySet().stream(), second.entrySet().stream())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum));
  }

  private <T> RecommendationContent buildRecommendationContent(T candidate, long count) {
    // Set filters for platform
    SearchParams searchParams =
        new SearchParams()
            .setQuery("")
            .setFilters(
                new CriterionArray(
                    ImmutableList.of(
                        new Criterion()
                            .setField(getSearchFieldName())
                            .setValue(candidate.toString()))));
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
