package com.linkedin.datahub.graphql.mappers;

import com.linkedin.datahub.graphql.generated.OwnershipSource;
import com.linkedin.datahub.graphql.generated.OwnershipSourceType;

import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * Note that it is our intention to eventually auto-generate these types of boilerplate mappers.
 */
public class OwnershipSourceMapper implements ModelMapper<com.linkedin.common.OwnershipSource, OwnershipSource> {

    public static final OwnershipSourceMapper INSTANCE = new OwnershipSourceMapper();

    public static OwnershipSource map(@Nonnull final com.linkedin.common.OwnershipSource ownershipSource) {
        return INSTANCE.apply(ownershipSource);
    }

    @Override
    public OwnershipSource apply(@Nonnull final com.linkedin.common.OwnershipSource ownershipSource) {
        final OwnershipSource result = new OwnershipSource();
        result.setUrl(ownershipSource.getUrl());
        result.setType(Enum.valueOf(OwnershipSourceType.class, ownershipSource.getType().toString()));
        return result;
    }
}