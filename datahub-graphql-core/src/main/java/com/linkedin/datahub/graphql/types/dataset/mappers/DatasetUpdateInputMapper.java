package com.linkedin.datahub.graphql.types.dataset.mappers;

import javax.annotation.Nonnull;

import com.linkedin.datahub.graphql.generated.DatasetUpdateInput;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryUpdateMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DatasetDeprecation;

public class DatasetUpdateInputMapper implements ModelMapper<DatasetUpdateInput, Dataset> {

    public static final DatasetUpdateInputMapper INSTANCE = new DatasetUpdateInputMapper();

    public static Dataset map(@Nonnull final DatasetUpdateInput datasetUpdateInput) {
        return INSTANCE.apply(datasetUpdateInput);
    }

    @Override
    public Dataset apply(@Nonnull final DatasetUpdateInput datasetUpdateInput) {
        final Dataset result = new Dataset();

        if (datasetUpdateInput.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(datasetUpdateInput.getOwnership()));
        }

        if (datasetUpdateInput.getDeprecation() != null) {
            final DatasetDeprecation deprecation = new DatasetDeprecation();
            deprecation.setDeprecated(datasetUpdateInput.getDeprecation().getDeprecated());
            if (datasetUpdateInput.getDeprecation().getDecommissionTime() != null) {
                deprecation.setDecommissionTime(datasetUpdateInput.getDeprecation().getDecommissionTime());
            }
            deprecation.setNote(datasetUpdateInput.getDeprecation().getNote());
            result.setDeprecation(deprecation);
        }

        if (datasetUpdateInput.getInstitutionalMemory() != null) {
            result.setInstitutionalMemory(InstitutionalMemoryUpdateMapper.map(datasetUpdateInput.getInstitutionalMemory()));
        }

        return result;
    }
}
