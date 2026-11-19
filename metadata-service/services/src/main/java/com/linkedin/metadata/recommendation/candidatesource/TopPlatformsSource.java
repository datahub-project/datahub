package com.linkedin.metadata.recommendation.candidatesource;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TopPlatformsSource extends EntitySearchAggregationSource {

  /**
   * Platform-bearing entity types considered when aggregating top platforms. At request time this
   * is narrowed to the default search entity types, see {@link
   * #getPlatformEntityNames(OperationContext)}.
   */
  private static final List<String> PLATFORM_ENTITY_TYPES =
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
          Constants.NOTEBOOK_ENTITY_NAME,
          Constants.REPOSITORY_ENTITY_NAME,
          Constants.SERVICE_ENTITY_NAME,
          Constants.API_ENTITY_NAME,
          Constants.AGENT_SKILL_ENTITY_NAME,
          Constants.AI_AGENT_ENTITY_NAME);

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
  public List<RecommendationContent> getRecommendations(
      @Nonnull OperationContext opContext,
      @Nullable RecommendationRequestContext requestContext,
      @Nullable Filter filter) {
    // An empty entity list makes the aggregation count every entity type, so skip it instead.
    if (getPlatformEntityNames(opContext).isEmpty()) {
      return Collections.emptyList();
    }
    return super.getRecommendations(opContext, requestContext, filter);
  }

  @Override
  protected List<String> getEntityNames(
      @Nonnull OperationContext opContext, EntityRegistry entityRegistry) {
    return getPlatformEntityNames(opContext);
  }

  /**
   * Only count entity types that default search returns. A platform tile opens a search with just a
   * platform filter, so counting a type outside the default search types would show assets on the
   * tile that the click-through search cannot find. An unresolved default list is treated as empty,
   * matching how GraphQL search resolves it.
   */
  private static List<String> getPlatformEntityNames(@Nonnull OperationContext opContext) {
    final List<String> defaultSearchEntityNames =
        opContext.getSearchContext().getDefaultSearchEntityNames();
    if (defaultSearchEntityNames == null) {
      log.warn(
          "Default search entity types were not resolved onto SearchContext; the Platforms module"
              + " has no entity types to count. Ensure elasticsearch.search.defaultEntityTypes is"
              + " configured.");
      return Collections.emptyList();
    }
    final Set<String> searchable = new HashSet<>(defaultSearchEntityNames);
    return PLATFORM_ENTITY_TYPES.stream().filter(searchable::contains).collect(Collectors.toList());
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
