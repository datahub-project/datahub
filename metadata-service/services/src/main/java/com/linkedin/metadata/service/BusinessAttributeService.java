package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessAttributeService {
  private final EntityClient _entityClient;

  public BusinessAttributeService(EntityClient entityClient) {
    _entityClient = entityClient;
  }

  public EntityResponse getBusinessAttributeEntityResponse(
      @Nonnull final Urn businessAttributeUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(businessAttributeUrn, "business attribute must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return _entityClient
          .batchGetV2(
              Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
              Set.of(businessAttributeUrn),
              Set.of(Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME),
              authentication)
          .get(businessAttributeUrn);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Business Attribute with urn %s", businessAttributeUrn),
          e);
    }
  }
}
