package com.linkedin.metadata.recommendation;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.candidatesource.RecommendationSource;
import com.linkedin.metadata.recommendation.ranker.RecommendationModuleRanker;
import com.linkedin.metadata.utils.ConcurrencyUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecommendationsService {

  private final List<RecommendationSource> _candidateSources;
  private final RecommendationModuleRanker _moduleRanker;

  public RecommendationsService(
      final List<RecommendationSource> candidateSources,
      final RecommendationModuleRanker moduleRanker) {
    validateRecommendationSources(candidateSources);
    _candidateSources = candidateSources;
    _moduleRanker = moduleRanker;
  }

  private void validateRecommendationSources(final List<RecommendationSource> candidateSources) {
    final Map<String, Long> moduleIdCount =
        candidateSources.stream()
            .collect(
                Collectors.groupingBy(RecommendationSource::getModuleId, Collectors.counting()));
    List<String> moduleIdsWithDuplicates =
        moduleIdCount.entrySet().stream()
            .filter(entry -> entry.getValue() > 1)
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());
    if (!moduleIdsWithDuplicates.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "Found recommendations candidate sources with duplicate module IDs: %s",
              moduleIdsWithDuplicates.toString()));
    }
  }

  private String standardizeModuleId(final String moduleId) {
    return moduleId.toLowerCase(Locale.ROOT).replaceAll("[^a-zA-Z]", "");
  }

  private boolean filterByModuleId(Set<String> requestedModules, RecommendationSource source) {
    if (requestedModules == null || requestedModules.isEmpty()) {
      return true;
    }
    return requestedModules.contains(standardizeModuleId(source.getModuleId()));
  }

  /**
   * Return the list of recommendation modules given input context
   *
   * @param opContext User's context requesting recommendations
   * @param requestContext Context of where the recommendations are being requested
   * @param limit Max number of modules to return
   * @return List of recommendation modules
   */
  @Nonnull
  @WithSpan
  public List<RecommendationModule> listRecommendations(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nullable Filter filter,
      int limit) {

    Set<String> requestedModules =
        requestContext.hasModules()
            ? requestContext.getModules().stream()
                .map(moduleId -> standardizeModuleId(moduleId.toString()))
                .collect(Collectors.toSet())
            : null;

    // Get recommendation candidates from sources which are eligible, in parallel
    final List<RecommendationModule> candidateModules =
        ConcurrencyUtils.transformAndCollectAsync(
                _candidateSources.stream()
                    .filter(source -> source.isEligible(opContext, requestContext))
                    .filter(source -> filterByModuleId(requestedModules, source))
                    .collect(Collectors.toList()),
                source -> source.getRecommendationModule(opContext, requestContext, filter),
                (source, exception) -> {
                  log.error(
                      "Error while fetching candidate modules from source {}", source, exception);
                  return Optional.<RecommendationModule>empty();
                })
            .stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());

    // Rank recommendation modules, which determines their ordering during rendering
    return _moduleRanker.rank(opContext, requestContext, candidateModules, limit);
  }
}
