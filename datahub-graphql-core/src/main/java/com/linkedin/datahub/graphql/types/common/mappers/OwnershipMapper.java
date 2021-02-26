package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.Ownership;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
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
