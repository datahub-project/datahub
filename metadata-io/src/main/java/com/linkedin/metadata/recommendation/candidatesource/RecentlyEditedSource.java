package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableSet;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationDataAccess;
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

@RequiredArgsConstructor
public class RecentlyEditedSource implements EntityRecommendationSource {
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

  private static final int MAX_CONTENT = 5;

  @Nullable private final UsageEventsRecommendationDataAccess usageEventsDataAccess;
  private final EntityService<?> entityService;

  @Override
  public String getTitle() {
    return "Recently Edited";
  }

  @Override
  public String getModuleId() {
    return "RecentlyEditedEntities";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.ENTITY_NAME_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME
        && usageEventsDataAccess != null
        && usageEventsDataAccess.isDataAvailable(opContext);
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext,
      @Nonnull RecommendationRequestContext requestContext,
      @Nullable Filter filter) {
    return opContext.withSpan(
        "getRecentlyEdited",
        () -> {
          if (usageEventsDataAccess == null) {
            return List.of();
          }
          List<String> bucketUrns = usageEventsDataAccess.recentlyEditedEntityUrns(opContext);
          return buildContent(opContext, bucketUrns, entityService)
              .limit(MAX_CONTENT)
              .collect(Collectors.toList());
        },
        MetricUtils.DROPWIZARD_NAME,
        MetricUtils.name(this.getClass(), "getRecentlyEdited"));
  }

  @Override
  public Set<String> getSupportedEntityTypes() {
    return SUPPORTED_ENTITY_TYPES;
  }
}
