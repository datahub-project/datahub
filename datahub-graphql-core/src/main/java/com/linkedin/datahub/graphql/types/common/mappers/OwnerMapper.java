package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.Owner;
import com.linkedin.datahub.graphql.generated.OwnershipType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class OwnerMapper implements ModelMapper<com.linkedin.common.Owner, Owner> {

    public static final OwnerMapper INSTANCE = new OwnerMapper();

    public static Owner map(@Nonnull final com.linkedin.common.Owner owner) {
        return INSTANCE.apply(owner);
    }

    @Override
    public Owner apply(@Nonnull final com.linkedin.common.Owner owner) {
        final Owner result = new Owner();
        result.setType(Enum.valueOf(OwnershipType.class, owner.getType().toString()));
        if (owner.getOwner().getEntityType().equals("corpuser")) {
            CorpUser partialOwner = new CorpUser();
            partialOwner.setUrn(owner.getOwner().toString());
            result.setOwner(partialOwner);
        } else {
            CorpGroup partialOwner = new CorpGroup();
            partialOwner.setUrn(owner.getOwner().toString());
            result.setOwner(partialOwner);
        }
        if (owner.hasSource()) {
            result.setSource(OwnershipSourceMapper.map(owner.getSource()));
        }
        return result;
    }
}
