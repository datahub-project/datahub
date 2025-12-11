/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.businessattribute.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.BusinessAttribute;
import com.linkedin.datahub.graphql.generated.BusinessAttributeAssociation;
import com.linkedin.datahub.graphql.generated.BusinessAttributes;
import com.linkedin.datahub.graphql.generated.EntityType;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BusinessAttributesMapper {

  private static final Logger _logger =
      LoggerFactory.getLogger(BusinessAttributesMapper.class.getName());
  public static final BusinessAttributesMapper INSTANCE = new BusinessAttributesMapper();

  public static BusinessAttributes map(
      @Nonnull final com.linkedin.businessattribute.BusinessAttributes businessAttributes,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(businessAttributes, entityUrn);
  }

  private BusinessAttributes apply(
      @Nonnull com.linkedin.businessattribute.BusinessAttributes businessAttributes,
      @Nonnull Urn entityUrn) {
    final BusinessAttributes result = new BusinessAttributes();
    result.setBusinessAttribute(
        mapBusinessAttributeAssociation(businessAttributes.getBusinessAttribute(), entityUrn));
    return result;
  }

  private BusinessAttributeAssociation mapBusinessAttributeAssociation(
      com.linkedin.businessattribute.BusinessAttributeAssociation businessAttributeAssociation,
      Urn entityUrn) {
    if (Objects.isNull(businessAttributeAssociation)) {
      return null;
    }
    final BusinessAttributeAssociation businessAttributeAssociationResult =
        new BusinessAttributeAssociation();
    final BusinessAttribute businessAttribute = new BusinessAttribute();
    businessAttribute.setUrn(businessAttributeAssociation.getBusinessAttributeUrn().toString());
    businessAttribute.setType(EntityType.BUSINESS_ATTRIBUTE);
    businessAttributeAssociationResult.setBusinessAttribute(businessAttribute);
    businessAttributeAssociationResult.setAssociatedUrn(entityUrn.toString());
    return businessAttributeAssociationResult;
  }
}
