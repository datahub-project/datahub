package com.linkedin.datahub.graphql.types.common.mappers;

import java.util.stream.Collectors;

import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.datahub.graphql.generated.OwnershipUpdate;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import lombok.NonNull;

public class OwnershipUpdateMapper implements ModelMapper<OwnershipUpdate, Ownership> {

    private static final OwnershipUpdateMapper INSTANCE = new OwnershipUpdateMapper();

    public static Ownership map(@NonNull final OwnershipUpdate input) {
        return INSTANCE.apply(input);
    }

    @Override
    public Ownership apply(@NonNull final OwnershipUpdate input) {
        final Ownership ownership = new Ownership();

        ownership.setOwners(new OwnerArray(input.getOwners()
            .stream()
            .map(OwnerUpdateMapper::map)
            .collect(Collectors.toList())));

        return ownership;
    }
}
