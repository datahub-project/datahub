package com.linkedin.datahub.graphql.types.domain;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainAssociation;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.MetadataAttributionMapper;
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
    if (domains.getDomainAssociations() != null && !domains.getDomainAssociations().isEmpty()) {
      com.linkedin.domain.DomainAssociation association =
          domains.getDomainAssociations().getFirst();
      Urn domainUrn = association.getDomain();
      if (context == null || canView(context.getOperationContext(), domainUrn)) {
        DomainAssociation gqlAssociation = new DomainAssociation();
        gqlAssociation.setDomain(
            Domain.builder().setType(EntityType.DOMAIN).setUrn(domainUrn.toString()).build());
        gqlAssociation.setAssociatedUrn(entityUrn);
        if (association.getAttribution() != null) {
          gqlAssociation.setAttribution(
              MetadataAttributionMapper.map(context, association.getAttribution()));
        }
        return gqlAssociation;
      }
    }
    return null;
  }
}
