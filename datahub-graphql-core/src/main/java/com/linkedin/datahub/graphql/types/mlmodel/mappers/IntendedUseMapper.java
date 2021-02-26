package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import com.linkedin.datahub.graphql.generated.IntendedUse;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import lombok.NonNull;

public class IntendedUseMapper implements ModelMapper<com.linkedin.ml.metadata.IntendedUse,IntendedUse> {

    public static final IntendedUseMapper INSTANCE = new IntendedUseMapper();

    public static IntendedUse map(@NonNull final com.linkedin.ml.metadata.IntendedUse intendedUse) {
        return INSTANCE.apply(intendedUse);
    }

    @Override
    public IntendedUse apply(@NonNull final com.linkedin.ml.metadata.IntendedUse intendedUse) {
        final IntendedUse result = new IntendedUse();
        result.setOutOfScopeUses(intendedUse.getOutOfScopeUses());
        result.setPrimaryUses(intendedUse.getPrimaryUses());
        result.setPrimaryUsers(intendedUse.getPrimaryUsers());
        return result;
    }
}
