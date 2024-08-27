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
public class TopTermsSource extends EntitySearchAggregationSource {

  private static final String TERMS = "glossaryTerms";

  public TopTermsSource(EntitySearchService entitySearchService, EntityRegistry entityRegistry) {
    super(entitySearchService, entityRegistry);
  }

  @Override
  public String getTitle() {
    return "Top Glossary Terms";
  }

  @Override
  public String getModuleId() {
    return "TopTerms";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.GLOSSARY_TERM_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull OperationContext opContext, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME
        || requestContext.getScenario() == ScenarioType.SEARCH_RESULTS;
  }

  @Override
  protected String getSearchFieldName() {
    return TERMS;
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
