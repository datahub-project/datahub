package com.linkedin.datahub.graphql.types.restricted;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.entity.EntityResponse;
import io.datahubproject.metadata.services.RestrictedService;
import javax.annotation.Nonnull;

public class RestrictedMapper {

  public static final RestrictedMapper INSTANCE = new RestrictedMapper();

  public static Restricted map(
      @Nonnull final EntityResponse entityResponse,
      @Nonnull final RestrictedService restrictedService) {
    return INSTANCE.apply(entityResponse, restrictedService);
  }

  public Restricted apply(
      @Nonnull final EntityResponse entityResponse,
      @Nonnull final RestrictedService restrictedService) {
    final Restricted result = new Restricted();
    Urn entityUrn = entityResponse.getUrn();
    String restrictedUrnString = restrictedService.encryptRestrictedUrn(entityUrn).toString();

    result.setUrn(restrictedUrnString);
    result.setType(EntityType.RESTRICTED);

    return result;
  }
}
