package com.linkedin.metadata.recommendation.candidatesource;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.recommendation.EntityProfileParams;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public interface EntityRecommendationSource extends RecommendationSource {
  Set<String> getSupportedEntityTypes();

  default RecommendationContent buildContent(@Nonnull Urn urn) {
    return new RecommendationContent()
        .setEntity(urn)
        .setValue(urn.toString())
        .setParams(
            new RecommendationParams()
                .setEntityProfileParams(new EntityProfileParams().setUrn(urn)));
  }

  default Stream<RecommendationContent> buildContent(
      @Nonnull OperationContext opContext,
      @Nonnull List<String> entityUrns,
      EntityService<?> entityService) {
    List<Urn> entities =
        entityUrns.stream()
            .map(UrnUtils::getUrn)
            .filter(urn -> getSupportedEntityTypes().contains(urn.getEntityType()))
            .collect(Collectors.toList());
    Set<Urn> existingNonRemoved = entityService.exists(opContext, entities, false);

    return entities.stream().filter(existingNonRemoved::contains).map(this::buildContent);
  }
}
