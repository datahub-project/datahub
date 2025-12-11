/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.service;

import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessAttributeService {
  private final EntityService<?> entityService;

  public BusinessAttributeService(EntityService<?> entityService) {
    this.entityService = entityService;
  }

  public EntityResponse getBusinessAttributeEntityResponse(
      @Nonnull final OperationContext opContext, @Nonnull final Urn businessAttributeUrn) {
    Objects.requireNonNull(businessAttributeUrn, "business attribute must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");
    try {
      return entityService.getEntityV2(
          opContext,
          Constants.BUSINESS_ATTRIBUTE_ENTITY_NAME,
          businessAttributeUrn,
          Set.of(Constants.BUSINESS_ATTRIBUTE_INFO_ASPECT_NAME));
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Business Attribute with urn %s", businessAttributeUrn),
          e);
    }
  }
}
