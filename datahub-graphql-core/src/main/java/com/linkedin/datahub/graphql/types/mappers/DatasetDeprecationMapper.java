package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.DatasetDeprecation;

import javax.annotation.Nonnull;

public class DatasetDeprecationMapper implements ModelMapper<com.linkedin.dataset.DatasetDeprecation, DatasetDeprecation> {

    public static final DatasetDeprecationMapper INSTANCE = new DatasetDeprecationMapper();

    public static DatasetDeprecation map(@Nonnull final com.linkedin.dataset.DatasetDeprecation deprecation) {
        return INSTANCE.apply(deprecation);
    }

    @Override
    public DatasetDeprecation apply(@Nonnull final com.linkedin.dataset.DatasetDeprecation input) {
        final DatasetDeprecation result = new DatasetDeprecation();
        result.setActor(input.getActor().toString());
        result.setDeprecated(input.isDeprecated());
        result.setDecommissionTime(input.getDecommissionTime());
        result.setNote(input.getNote());
        return result;
    }
}
