package com.linkedin.datahub.graphql.resolvers.lifecycle;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Maps a {@link LifecycleStageTypeInfo} PDL record to the GraphQL {@link LifecycleStageType}. */
public final class LifecycleStageTypeMapper {

  public static final String ENTITY_NAME = "lifecycleStageType";
  public static final String INFO_ASPECT = "lifecycleStageTypeInfo";

  private LifecycleStageTypeMapper() {}

  @Nonnull
  public static LifecycleStageType map(@Nonnull String urn, @Nonnull LifecycleStageTypeInfo info) {
    LifecycleStageType result = new LifecycleStageType();
    result.setUrn(urn);
    result.setName(info.getName());
    result.setDescription(info.hasDescription() ? info.getDescription() : null);
    result.setHideInSearch(info.getSettings().isHideInSearch());
    result.setEntityTypes(info.hasEntityTypes() ? new ArrayList<>(info.getEntityTypes()) : null);

    if (info.hasTransitionPolicy()) {
      LifecycleStageTransitionPolicy policy = info.getTransitionPolicy();
      if (policy.hasAllowedPreviousStages()) {
        result.setAllowedPreviousStages(
            policy.getAllowedPreviousStages().stream()
                .map(Urn::toString)
                .collect(Collectors.toList()));
      }
    }

    return result;
  }
}
