package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DomainsCandidateSource extends EntitySearchAggregationSource {

  private static final String DOMAINS = "domains";

  public DomainsCandidateSource(EntitySearchService entitySearchService) {
    super(entitySearchService);
  }

  @Override
  public String getTitle() {
    return "Domains";
  }

  @Override
  public String getModuleId() {
    return "Domains";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.DOMAIN_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME;
  }

  @Override
  protected String getSearchFieldName() {
    return DOMAINS;
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
