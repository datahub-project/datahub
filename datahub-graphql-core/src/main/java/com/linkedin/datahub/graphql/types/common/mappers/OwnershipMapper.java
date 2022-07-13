package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.Ownership;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class OwnershipMapper {

    public static final OwnershipMapper INSTANCE = new OwnershipMapper();

    public static Ownership map(@Nonnull final com.linkedin.common.Ownership ownership, @Nonnull final Urn entityUrn) {
        return INSTANCE.apply(ownership, entityUrn);
    }

    public Ownership apply(@Nonnull final com.linkedin.common.Ownership ownership, @Nonnull final Urn entityUrn) {
        final Ownership result = new Ownership();
        result.setLastModified(AuditStampMapper.map(ownership.getLastModified()));
        result.setOwners(ownership.getOwners()
                .stream()
                .map(owner -> OwnerMapper.map(owner, entityUrn))
                .collect(Collectors.toList()));
        return result;
    }
}
