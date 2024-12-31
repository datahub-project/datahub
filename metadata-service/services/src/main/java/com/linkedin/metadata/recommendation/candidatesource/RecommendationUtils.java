package com.linkedin.metadata.recommendation.candidatesource;

import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import javax.annotation.Nonnull;

public class RecommendationUtils {

  /**
   * Returns true if a given URN is in a fixed set of entity types, false otherwise.
   *
   * @param urn the urn to check
   * @param entityTypes the set of valid entity types
   * @return true if the type of the urn is in the set of valid entity types, false otherwise.
   */
  public static boolean isSupportedEntityType(
      @Nonnull OperationContext opContext, @Nonnull final Set<String> entityTypes) {
    final String entityType = opContext.getActorContext().getActorUrn().getEntityType();
    return entityTypes.contains(entityType);
  }

  private RecommendationUtils() {}
}
