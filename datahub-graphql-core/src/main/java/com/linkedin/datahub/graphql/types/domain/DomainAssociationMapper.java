/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.domain;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainAssociation;
import com.linkedin.datahub.graphql.generated.EntityType;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class DomainAssociationMapper {

  public static final DomainAssociationMapper INSTANCE = new DomainAssociationMapper();

  public static DomainAssociation map(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.domain.Domains domains,
      @Nonnull final String entityUrn) {
    return INSTANCE.apply(context, domains, entityUrn);
  }

  public DomainAssociation apply(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.domain.Domains domains,
      @Nonnull final String entityUrn) {
    if (domains.getDomains().size() > 0
        && (context == null
            || canView(context.getOperationContext(), domains.getDomains().get(0)))) {
      DomainAssociation association = new DomainAssociation();
      association.setDomain(
          Domain.builder()
              .setType(EntityType.DOMAIN)
              .setUrn(domains.getDomains().get(0).toString())
              .build());
      association.setAssociatedUrn(entityUrn);
      return association;
    }
    return null;
  }
}
