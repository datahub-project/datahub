package com.linkedin.datahub.graphql.types.dataflow.mappers;

import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datajob.DataJobsLineage;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;

public class DataJobsLineageMapper implements ModelMapper<DataJobsLineage, List<DataJob>> {

    public static final DataJobsLineageMapper INSTANCE = new DataJobsLineageMapper();

    public static List<DataJob> map(@Nonnull final DataJobsLineage lineage) {
        return INSTANCE.apply(lineage);
    }

    @Override
    public List<DataJob> apply(@Nonnull final DataJobsLineage input) {
        return input.getDataJobs().stream().map(urn -> {
            final DataJob dataJob = new DataJob();
            dataJob.setUrn(urn.toString());
            return dataJob;
        }).collect(Collectors.toList());
    }
}
