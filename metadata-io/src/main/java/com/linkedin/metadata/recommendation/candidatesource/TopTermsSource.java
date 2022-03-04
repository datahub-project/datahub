package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.RecommendationModuleKey;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TopTermsSource extends EntitySearchAggregationSource implements RecommendationSourceWithOffline {

  private final EntityService _entityService;
  private final boolean _fetchOffline;

  private static final String TERMS = "glossaryTerms";

  private static final String MODULE_ID = "TopTerms";
  private static final Urn MODULE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(new RecommendationModuleKey().setModuleId(MODULE_ID).setIdentifier("GLOBAL"),
          Constants.RECOMMENDATION_MODULE_ENTITY_NAME);

  public TopTermsSource(EntitySearchService entitySearchService, EntityService entityService, boolean fetchOffline) {
    super(entitySearchService);
    _entityService = entityService;
    _fetchOffline = fetchOffline;
  }

  @Override
  public String getTitle() {
    return "Top Glossary Terms";
  }

  @Override
  public String getModuleId() {
    return MODULE_ID;
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.GLOSSARY_TERM_SEARCH_LIST;
  }

  @Override
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
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

  @Override
  public EntityService getEntityService() {
    return _entityService;
  }

  @Override
  public boolean shouldFetchFromOffline() {
    return _fetchOffline;
  }

  @Override
  public Urn getRecommendationModuleUrn(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return MODULE_URN;
  }
}
