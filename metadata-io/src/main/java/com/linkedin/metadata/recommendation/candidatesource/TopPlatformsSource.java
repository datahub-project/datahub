package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.EntitySearchService;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;


@Slf4j
public class TopPlatformsSource extends EntitySearchAggregationSource {
  private final EntityService _entityService;

  private static final String PLATFORM = "platform";

  public TopPlatformsSource(EntityService entityService, EntitySearchService entitySearchService,
      EntityRegistry entityRegistry, CacheManager cacheManager) {
    super(entitySearchService, entityRegistry, cacheManager);
    _entityService = entityService;
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
  protected String getSearchFieldName() {
    return PLATFORM;
  }

  @Override
  protected int getMaxContent() {
    return 20;
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
