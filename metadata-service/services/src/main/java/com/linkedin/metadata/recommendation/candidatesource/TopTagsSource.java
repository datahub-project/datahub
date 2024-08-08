package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopTagsSource extends EntitySearchAggregationSource {

  private static final String TAGS = "tags";

  public TopTagsSource(EntitySearchService entitySearchService, EntityRegistry entityRegistry) {
    super(entitySearchService, entityRegistry);
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
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME
        || requestContext.getScenario() == ScenarioType.SEARCH_RESULTS;
  }

  @Override
  protected String getSearchFieldName() {
    return TAGS;
  }

  @Override
  protected int getMaxContent() {
    return 10;
  }

  @Override
  protected boolean isValueUrn() {
    return true;
  }
}
