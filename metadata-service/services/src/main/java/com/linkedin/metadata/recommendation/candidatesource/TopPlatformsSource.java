package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopPlatformsSource extends EntitySearchAggregationSource {

  /**
   * Entity types considered when aggregating top platforms. Intentionally a platform-bearing
   * subset, not the full search default entity-type list.
   */
  private static final List<String> SEARCHABLE_ENTITY_TYPES =
      ImmutableList.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME,
          Constants.ML_FEATURE_TABLE_ENTITY_NAME,
          Constants.ML_FEATURE_ENTITY_NAME,
          Constants.ML_PRIMARY_KEY_ENTITY_NAME,
          Constants.DATA_FLOW_ENTITY_NAME,
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.TAG_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME,
          Constants.NOTEBOOK_ENTITY_NAME);

  private static final String PLATFORM = "platform";

  public TopPlatformsSource(
      EntitySearchService entitySearchService,
      EntityService<?> entityService,
      EntityRegistry entityRegistry) {
    super(entityService, entitySearchService, entityRegistry);
  }

  @Override
  public String getTitle() {
    return "Platforms";
  }

  @Override
  public String getModuleId() {
    return "Platforms";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.PLATFORM_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME;
  }

  @Override
  protected List<String> getEntityNames(EntityRegistry entityRegistry) {
    return SEARCHABLE_ENTITY_TYPES;
  }

  @Override
  protected String getSearchFieldName() {
    return PLATFORM;
  }

  @Override
  protected int getMaxContent() {
    return 40;
  }

  @Override
  protected boolean isValueUrn() {
    return true;
  }

  // Note: we intentionally do NOT override getValidCandidateUrns to require a logoUrl. A platform
  // with ingested assets but no dataPlatformInfo/logoUrl (e.g. a connector whose platform is not
  // yet in the data-platforms bootstrap seed) must still surface here — the UI renders a default
  // platform icon when a logo is missing. Filtering on logoUrl silently hid such platforms from
  // the home page even though they were searchable. The base implementation's existence check is
  // the only validation we want.
}
