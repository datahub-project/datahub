package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.services.RestrictedService;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utility class for creating Restricted entity results when a user lacks permission to view an
 * entity. This provides a consistent way for entity types to return Restricted entities instead of
 * null.
 */
public class RestrictedResultMapper {

  private RestrictedResultMapper() {}

  /**
   * Creates a DataFetcherResult containing a Restricted entity for the given URN. The URN will be
   * encrypted using the RestrictedService if provided.
   *
   * @param urn The original entity URN
   * @param restrictedService Service to encrypt the URN (may be null)
   * @return A DataFetcherResult containing the Restricted entity (typed for compatibility)
   */
  @SuppressWarnings("unchecked")
  public static <T> DataFetcherResult<T> createRestrictedResult(
      @Nonnull Urn urn, @Nullable RestrictedService restrictedService) {
    Restricted restricted = new Restricted();
    restricted.setType(EntityType.RESTRICTED);

    if (restrictedService != null) {
      restricted.setUrn(restrictedService.encryptRestrictedUrn(urn).toString());
    } else {
      // Fallback: use original URN if RestrictedService not available
      restricted.setUrn(urn.toString());
    }

    return (DataFetcherResult<T>) DataFetcherResult.newResult().data(restricted).build();
  }
}
