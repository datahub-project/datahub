package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInfo;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DataJobMapper implements ModelMapper<com.linkedin.datajob.DataJob, DataJob> {

    public static final DataJobMapper INSTANCE = new DataJobMapper();

    public static DataJob map(@Nonnull final com.linkedin.datajob.DataJob dataJob) {
        return INSTANCE.apply(dataJob);
    }

    @Override
    public DataJob apply(@Nonnull final com.linkedin.datajob.DataJob dataJob) {
        final DataJob result = new DataJob();
        result.setUrn(dataJob.getUrn().toString());
        result.setType(EntityType.DATA_JOB);
        result.setDataFlow(new DataFlow.Builder().setUrn(dataJob.getDataFlow().toString()).build());
        result.setJobId(dataJob.getJobId());
        if (dataJob.hasInfo()) {
            result.setInfo(mapDataJobInfo(dataJob.getInfo()));
        }
        if (dataJob.hasInputOutput()) {
            result.setInputOutput(mapDataJobInputOutput(dataJob.getInputOutput()));
        }
        if (dataJob.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(dataJob.getOwnership()));
        }
        if (dataJob.hasStatus()) {
            result.setStatus(StatusMapper.map(dataJob.getStatus()));
        }
        if (dataJob.hasGlobalTags()) {
            result.setGlobalTags(GlobalTagsMapper.map(dataJob.getGlobalTags()));
        }
        return result;
    }

    private DataJobInfo mapDataJobInfo(final com.linkedin.datajob.DataJobInfo info) {
        final DataJobInfo result = new DataJobInfo();
        result.setName(info.getName());
        result.setDescription(info.getDescription());
        if (info.hasExternalUrl()) {
            result.setExternalUrl(info.getExternalUrl().toString());
        }
        if (info.hasCustomProperties()) {
            result.setCustomProperties(StringMapMapper.map(info.getCustomProperties()));
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
