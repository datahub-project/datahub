package com.linkedin.datahub.graphql.resolvers.lifecycle;

import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.LifecycleStageType;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.lifecycle.LifecycleStageTransitionPolicy;
import com.linkedin.lifecycle.LifecycleStageTypeInfo;
import java.util.ArrayList;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Maps a {@link LifecycleStageTypeInfo} PDL record to the GraphQL {@link LifecycleStageType}. */
public final class LifecycleStageTypeMapper {

  public static final String ENTITY_NAME = "lifecycleStageType";
  public static final String INFO_ASPECT = "lifecycleStageTypeInfo";

  private LifecycleStageTypeMapper() {}

  /**
   * Maps an {@link EntityResponse} to a GraphQL {@link LifecycleStageType}. Returns null if the
   * info aspect is absent.
   */
  @Nullable
  public static LifecycleStageType map(
      @Nullable QueryContext context, @Nonnull EntityResponse entityResponse) {
    if (!entityResponse.getAspects().containsKey(INFO_ASPECT)) {
      return null;
    }
    LifecycleStageTypeInfo info =
        new LifecycleStageTypeInfo(entityResponse.getAspects().get(INFO_ASPECT).getValue().data());
    final LifecycleStageType result = map(entityResponse.getUrn().toString(), info);

    final EnvelopedAspect envelopedStatus = entityResponse.getAspects().get(STATUS_ASPECT_NAME);
    if (envelopedStatus != null) {
      result.setStatus(StatusMapper.map(context, new Status(envelopedStatus.getValue().data())));
    }

    return result;
  }

  @Nonnull
  public static LifecycleStageType map(@Nonnull String urn, @Nonnull LifecycleStageTypeInfo info) {
    LifecycleStageType result = new LifecycleStageType();
    result.setUrn(urn);
    result.setName(info.getName());
    result.setDescription(info.hasDescription() ? info.getDescription() : null);
    result.setUserAssignable(info.getSettings().isUserAssignable());
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
