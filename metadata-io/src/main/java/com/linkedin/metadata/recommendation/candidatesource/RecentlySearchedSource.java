package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.metadata.datahubusage.RecentSearchRecommendationAccess;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.recommendation.SearchParams;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RecentlySearchedSource implements RecommendationSource {

  private static final int MAX_CONTENT = 5;

  @Nullable private final RecentSearchRecommendationAccess recentSearchAccess;

  @Override
  public String getTitle() {
    return "Recent searches";
  }

  @Override
  public String getModuleId() {
    return "RecentSearches";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.SEARCH_QUERY_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.SEARCH_BAR
        && recentSearchAccess != null
        && recentSearchAccess.isDataAvailable(opContext);
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nullable Filter filter) {
    if (recentSearchAccess == null) {
      return Collections.emptyList();
    }
    return opContext.withSpan(
        "getRecentlySearched",
        () ->
            recentSearchAccess.recentSearchQueries(opContext).stream()
                .map(this::buildContent)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .limit(MAX_CONTENT)
                .collect(Collectors.toList()),
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getRecentlySearched"));
  }

  private boolean isQueryInvalid(@Nonnull String query) {
    return query.trim().isEmpty() || query.equals("*");
  }

  private Optional<RecommendationContent> buildContent(@Nonnull String query) {
    if (isQueryInvalid(query)) {
      return Optional.empty();
    }
    return Optional.of(
        new RecommendationContent()
            .setValue(query)
            .setParams(
                new RecommendationParams().setSearchParams(new SearchParams().setQuery(query))));
  }
}
