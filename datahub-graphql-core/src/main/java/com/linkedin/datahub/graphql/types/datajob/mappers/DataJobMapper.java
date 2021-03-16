package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInfo;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;


public class DataJobMapper implements ModelMapper<com.linkedin.datajob.DataJob, DataJob> {

    public static final DataJobMapper INSTANCE = new DataJobMapper();

    public static DataJob map(@Nonnull final com.linkedin.datajob.DataJob datajob) {
        return INSTANCE.apply(datajob);
    }

    @Override
    public DataJob apply(@Nonnull final com.linkedin.datajob.DataJob datajob) {
        final DataJob result = new DataJob();
        result.setUrn(datajob.getUrn().toString());
        result.setType(EntityType.DATAJOB);
        result.setDataFlow(new DataFlow.Builder().setUrn(datajob.getDataFlow().toString()).build());
        result.setJobId(datajob.getJobId());
        if (datajob.hasInfo()) {
            result.setInfo(mapDataJobInfo(datajob.getInfo()));
        }
        if (datajob.hasInputOutput()) {
            result.setInputOutput(mapDataJobInputOutput(datajob.getInputOutput()));
        }
        if (datajob.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(datajob.getOwnership()));
        }
        return result;
    }

    private DataJobInfo mapDataJobInfo(final com.linkedin.datajob.DataJobInfo info) {
        final DataJobInfo result = new DataJobInfo();
        result.setName(info.getName());
        if (info.hasDescription()) {
            result.setDescription(info.getDescription());
        }
        return result;
    }

    private DataJobInputOutput mapDataJobInputOutput(final com.linkedin.datajob.DataJobInputOutput inputOutput) {
        final DataJobInputOutput result = new DataJobInputOutput();
        result.setInputDatasets(inputOutput.getInputDatasets().stream().map(urn -> {
            final Dataset dataset = new Dataset();
            dataset.setUrn(urn.toString());
            return dataset;
        }).collect(Collectors.toList()));
        result.setOutputDatasets(inputOutput.getOutputDatasets().stream().map(urn -> {
            final Dataset dataset = new Dataset();
            dataset.setUrn(urn.toString());
            return dataset;
        }).collect(Collectors.toList()));

        return result;
    }
}


