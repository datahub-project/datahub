package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

@Getter
@RequiredArgsConstructor
public class TestSource implements RecommendationSource {

  private final String title;
  private final String moduleId;
  private final RecommendationRenderType renderType;
  private final boolean eligible;
  private final List<RecommendationContent> contents;

  @Override
  public String getTitle() {
    return title;
  }

  @Override
  public String getModuleId() {
    return moduleId;
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return renderType;
  }

  @Override
  public boolean isEligible(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return eligible;
  }

  @Override
  public List<RecommendationContent> getRecommendations(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    return contents;
  }
}
