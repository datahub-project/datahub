package com.linkedin.metadata.recommendation;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.candidatesource.RecommendationCandidateSource;
import com.linkedin.metadata.recommendation.ranker.RecommendationModuleRanker;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class RecommendationService {

  private final List<RecommendationCandidateSource> _candidateSources;
  private final RecommendationModuleRanker _ranker;

  public RecommendationService(List<RecommendationCandidateSource> candidateSources,
      RecommendationModuleRanker ranker) {
    validateCandidateSources(candidateSources);
    _candidateSources = candidateSources;
    _ranker = ranker;
  }

  private void validateCandidateSources(List<RecommendationCandidateSource> candidateSources) {
    Map<String, Long> moduleIdCount = candidateSources.stream()
        .collect(Collectors.groupingBy(RecommendationCandidateSource::getModuleId, Collectors.counting()));
    List<String> moduleIdsWithDuplicates = moduleIdCount.entrySet()
        .stream()
        .filter(entry -> entry.getValue() > 1)
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
    if (!moduleIdsWithDuplicates.isEmpty()) {
      log.error("Cannot have candidate sources with duplicate module IDs: {}", moduleIdsWithDuplicates);
      throw new IllegalArgumentException("Cannot have candidate sources with duplicate module IDs");
    }
  }

  /**
   * Return the list of recommendation modules given input context
   *
   * @param userUrn User requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @param limit Max number of modules to return
   * @return List of recommendation modules
   */
  @Nonnull
  public List<RecommendationModule> listRecommendations(@Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext, int limit) {
    // Get candidates from input candidate sources which are eligible in parallel
    List<RecommendationModule> candidates = ConcurrencyUtils.transformAndCollectAsync(_candidateSources.stream()
        .filter(source -> source.isEligible(userUrn, requestContext))
        .collect(Collectors.toList()), source -> source.getModule(userUrn, requestContext))
        .stream()
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    return _ranker.rank(candidates, userUrn, requestContext, limit);
  }
}
