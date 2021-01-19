package com.linkedin.datahub.graphql.mappers;

import com.linkedin.datahub.graphql.generated.Ownership;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * Note that it is our intention to eventually auto-generate these types of boilerplate mappers.
 */
public class OwnershipMapper implements ModelMapper<com.linkedin.common.Ownership, Ownership> {

    public static final OwnershipMapper INSTANCE = new OwnershipMapper();

    public static Ownership map(@Nonnull final com.linkedin.common.Ownership ownership) {
        return INSTANCE.apply(ownership);
    }

    @Override
    public Ownership apply(@Nonnull final com.linkedin.common.Ownership ownership) {
        final Ownership result = new Ownership();
        result.setLastModified(AuditStampMapper.map(ownership.getLastModified()));
        result.setOwners(ownership.getOwners()
                .stream()
                .map(OwnerMapper::map)
                .collect(Collectors.toList()));
        return result;
    }
}
