package com.linkedin.datahub.graphql.types.domain;

import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.DomainAssociation;
import com.linkedin.datahub.graphql.generated.EntityType;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class DomainAssociationMapper {

    public static final DomainAssociationMapper INSTANCE = new DomainAssociationMapper();

    public static DomainAssociation map(
        @Nonnull final com.linkedin.domain.Domains domains,
        @Nonnull final String entityUrn
    ) {
        return INSTANCE.apply(domains, entityUrn);
    }

    public DomainAssociation apply(@Nonnull final com.linkedin.domain.Domains domains, @Nonnull final String entityUrn) {
        if (domains.getDomains().size() > 0) {
            DomainAssociation association = new DomainAssociation();
            association.setDomain(Domain.builder()
                .setType(EntityType.DOMAIN)
                .setUrn(domains.getDomains().get(0).toString()).build());
            association.setAssociatedUrn(entityUrn);
            return association;
        }
        return null;
    }
}
