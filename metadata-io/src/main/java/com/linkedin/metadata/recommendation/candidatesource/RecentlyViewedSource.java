package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class RecentlyViewedSource implements EntityRecommendationSource {
  /** Entity Types that should be in scope for this type of recommendation. */
  private static final Set<String> SUPPORTED_ENTITY_TYPES =
      ImmutableSet.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.DATA_FLOW_ENTITY_NAME,
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_FEATURE_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME,
          Constants.ML_FEATURE_TABLE_ENTITY_NAME);

  private final UsageEventRecommendationBackend usageEvents;
  private final EntityService<?> entityService;

  private static final int MAX_CONTENT = 5;

  @Override
  public String getTitle() {
    return "Recently Viewed";
  }

  @Override
  public String getModuleId() {
    return "RecentlyViewedEntities";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.ENTITY_NAME_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME && usageEvents.isAvailable(opContext);
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nullable Filter filter) {
    return opContext.withSpan(
        "getRecentlyViewed",
        () -> {
          List<String> bucketUrns =
              usageEvents.recentEntityUrns(
                  opContext,
                  opContext.getSessionActorContext().getActorUrn(),
                  DataHubUsageEventType.ENTITY_VIEW_EVENT.getType(),
                  MAX_CONTENT);
          return buildContent(opContext, bucketUrns, entityService)
              .limit(MAX_CONTENT)
              .collect(Collectors.toList());
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getRecentlyViewed"));
  }

  @Override
  public Set<String> getSupportedEntityTypes() {
    return SUPPORTED_ENTITY_TYPES;
  }
}
