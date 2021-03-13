package com.linkedin.datahub.graphql.types.common.mappers;

import javax.annotation.Nonnull;

import com.linkedin.common.Owner;
import com.linkedin.common.OwnershipSource;
import com.linkedin.common.OwnershipSourceType;
import com.linkedin.common.OwnershipType;
import com.linkedin.datahub.graphql.generated.OwnerUpdate;
import com.linkedin.datahub.graphql.types.corpuser.CorpUserUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

public class OwnerUpdateMapper implements ModelMapper<OwnerUpdate, Owner> {

    private static final OwnerUpdateMapper INSTANCE = new OwnerUpdateMapper();

    public static Owner map(@Nonnull final OwnerUpdate input) {
        return INSTANCE.apply(input);
    }

    @Override
    public Owner apply(@Nonnull final OwnerUpdate input) {
        final Owner owner = new Owner();
        owner.setOwner(CorpUserUtils.getCorpUserUrn(input.getOwner()));
        owner.setType(OwnershipType.valueOf(input.getType().toString()));
        owner.setSource(new OwnershipSource().setType(OwnershipSourceType.SERVICE));
        return owner;
    }
}
