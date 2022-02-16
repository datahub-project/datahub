package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.RecommendationModuleKey;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationModule;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@RequiredArgsConstructor
public class MostPopularOfflineSource implements RecommendationSource {
  private final EntityService _entityService;

  private static final String MODULE_ID = "HighUsageEntities";
  private static final Urn MODULE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(new RecommendationModuleKey().setModuleId(MODULE_ID).setIdentifier("GLOBAL"),
          Constants.RECOMMENDATION_MODULE_ENTITY_NAME);

  @Override
  public String getTitle() {
    return "Most Popular";
  }

  @Override
  public String getModuleId() {
    return MODULE_ID;
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.ENTITY_NAME_LIST;
  }

  @Override
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return requestContext.getScenario() == ScenarioType.HOME;
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(@Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext) {
    EnvelopedAspect moduleAspect;
    try {
      moduleAspect = _entityService.getLatestEnvelopedAspect(Constants.RECOMMENDATION_MODULE_ENTITY_NAME, MODULE_URN,
          Constants.RECOMMENDATION_MODULE_ASPECT_NAME);
    } catch (Exception e) {
      log.error("Error while fetching recommendation module from enitty service", e);
      return Collections.emptyList();
    }

    RecommendationModule module = new RecommendationModule(moduleAspect.getValue().data());
    return module.getContent();
  }
}
