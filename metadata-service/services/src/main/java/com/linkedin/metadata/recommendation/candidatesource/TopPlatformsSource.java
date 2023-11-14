package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class TopPlatformsSource extends EntitySearchAggregationSource {

  /**
   * Set of entities that we want to consider for defining the top platform sources.
   * This must match SearchUtils.SEARCHABLE_ENTITY_TYPES
   */
  private static final Set<String> SEARCHABLE_ENTITY_TYPES = ImmutableSet.of(
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
      Constants.GLOSSARY_TERM_ENTITY_NAME,
      Constants.GLOSSARY_NODE_ENTITY_NAME,
      Constants.TAG_ENTITY_NAME,
      Constants.ROLE_ENTITY_NAME,
      Constants.CORP_USER_ENTITY_NAME,
      Constants.CORP_GROUP_ENTITY_NAME,
      Constants.CONTAINER_ENTITY_NAME,
      Constants.DOMAIN_ENTITY_NAME,
      Constants.DATA_PRODUCT_ENTITY_NAME,
      Constants.NOTEBOOK_ENTITY_NAME
  );
  private final Filter aggregationFilter;
  private final EntityService _entityService;
  private static final String PLATFORM = "platform";

  public TopPlatformsSource(EntityService entityService, EntitySearchService entitySearchService) {
    super(entitySearchService);
    _entityService = entityService;

    // Filter for platforms that only have the entities that have search cards
    Filter filter = new Filter();
    CriterionArray array = new CriterionArray(
        SEARCHABLE_ENTITY_TYPES.stream()
            .map(entityName -> new Criterion().setField("urn").setValue(entityName).setCondition(Condition.CONTAIN))
            .collect(Collectors.toList())
    );
    filter.setOr(new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    aggregationFilter = filter;
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
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME;
  }
  @Override
  protected Filter buildAggregationFilter() {
    return aggregationFilter;
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

  @Override
  protected boolean isValidCandidateUrn(Urn urn) {
    RecordTemplate dataPlatformInfo = _entityService.getLatestAspect(urn, "dataPlatformInfo");
    if (dataPlatformInfo == null) {
      return false;
    }
    return ((DataPlatformInfo) dataPlatformInfo).hasLogoUrl();
  }
}
