package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.GlobalTags;

import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobInfo;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataJobProperties;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.DataJobEditableProperties;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.DataJobSnapshot;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;


public class DataJobSnapshotMapper implements ModelMapper<DataJobSnapshot, DataJob> {

    public static final DataJobSnapshotMapper INSTANCE = new DataJobSnapshotMapper();

    public static DataJob map(@Nonnull final DataJobSnapshot dataJob) {
        return INSTANCE.apply(dataJob);
    }

    @Override
    public DataJob apply(@Nonnull final DataJobSnapshot dataJob) {
        final DataJob result = new DataJob();
        result.setUrn(dataJob.getUrn().toString());
        result.setType(EntityType.DATA_JOB);
        result.setDataFlow(new DataFlow.Builder().setUrn(dataJob.getUrn().getFlowEntity().toString()).build());
        result.setJobId(dataJob.getUrn().getJobIdEntity());

        ModelUtils.getAspectsFromSnapshot(dataJob).forEach(aspect -> {
            if (aspect instanceof com.linkedin.datajob.DataJobInfo) {
                com.linkedin.datajob.DataJobInfo info = com.linkedin.datajob.DataJobInfo.class.cast(aspect);
                result.setInfo(mapDataJobInfo(info));
                result.setProperties(mapDataJobInfoToProperties(info));
            } else if (aspect instanceof com.linkedin.datajob.DataJobInputOutput) {
                com.linkedin.datajob.DataJobInputOutput inputOutput = com.linkedin.datajob.DataJobInputOutput.class.cast(aspect);
                result.setInputOutput(mapDataJobInputOutput(inputOutput));
            } else if (aspect instanceof Ownership) {
                Ownership ownership = Ownership.class.cast(aspect);
                result.setOwnership(OwnershipMapper.map(ownership));
            } else if (aspect instanceof Status) {
                Status status = Status.class.cast(aspect);
                result.setStatus(StatusMapper.map(status));
            } else if (aspect instanceof GlobalTags) {
                result.setGlobalTags(GlobalTagsMapper.map(GlobalTags.class.cast(aspect)));
                result.setTags(GlobalTagsMapper.map(GlobalTags.class.cast(aspect)));
            } else if (aspect instanceof EditableDataJobProperties) {
                final DataJobEditableProperties dataJobEditableProperties = new DataJobEditableProperties();
                dataJobEditableProperties.setDescription(((EditableDataJobProperties) aspect).getDescription());
                result.setEditableProperties(dataJobEditableProperties);
            } else if (aspect instanceof InstitutionalMemory) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map((InstitutionalMemory) aspect));
            } else if (aspect instanceof GlossaryTerms) {
                result.setGlossaryTerms(GlossaryTermsMapper.map((GlossaryTerms) aspect));
            }
        });

        return result;
    }

    /**
     * Maps GMS {@link com.linkedin.datajob.DataJobInfo} to deprecated GraphQL {@link DataJobInfo}
     */
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

    /**
     * Maps GMS {@link com.linkedin.datajob.DataJobInfo} to new GraphQL {@link DataJobProperties}
     */
    private DataJobProperties mapDataJobInfoToProperties(final com.linkedin.datajob.DataJobInfo info) {
        final DataJobProperties result = new DataJobProperties();
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
        if (inputOutput.hasInputDatasets()) {
            result.setInputDatasets(inputOutput.getInputDatasets().stream().map(urn -> {
                final Dataset dataset = new Dataset();
                dataset.setUrn(urn.toString());
                return dataset;
            }).collect(Collectors.toList()));
        } else {
            result.setInputDatasets(ImmutableList.of());
        }
        if (inputOutput.hasOutputDatasets()) {
            result.setOutputDatasets(inputOutput.getOutputDatasets().stream().map(urn -> {
                final Dataset dataset = new Dataset();
                dataset.setUrn(urn.toString());
                return dataset;
            }).collect(Collectors.toList()));
        } else {
            result.setOutputDatasets(ImmutableList.of());
        }
        if (inputOutput.hasInputDatajobs()) {
            result.setInputDatajobs(inputOutput.getInputDatajobs().stream().map(urn -> {
                final DataJob dataJob = new DataJob();
                dataJob.setUrn(urn.toString());
                return dataJob;
            }).collect(Collectors.toList()));
        } else {
            result.setInputDatajobs(ImmutableList.of());
        }

        return result;
    }
}
