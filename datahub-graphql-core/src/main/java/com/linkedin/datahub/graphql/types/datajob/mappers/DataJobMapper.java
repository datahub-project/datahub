package com.linkedin.datahub.graphql.types.datajob.mappers;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Deprecation;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobEditableProperties;
import com.linkedin.datahub.graphql.generated.DataJobInfo;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataJobProperties;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Domain;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.key.DataJobKey;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


public class DataJobMapper implements ModelMapper<EntityResponse, DataJob> {

    public static final DataJobMapper INSTANCE = new DataJobMapper();

    public static DataJob map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public DataJob apply(@Nonnull final EntityResponse entityResponse) {
        final DataJob result = new DataJob();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.DATA_JOB);

        entityResponse.getAspects().forEach((name, aspect) -> {
            DataMap data = aspect.getValue().data();
            if (DATA_JOB_KEY_ASPECT_NAME.equals(name)) {
                final DataJobKey gmsKey = new DataJobKey(data);
                result.setDataFlow(new DataFlow.Builder().setUrn(gmsKey.getFlow().toString()).build());
                result.setJobId(gmsKey.getJobId());
            } else if (DATA_JOB_INFO_ASPECT_NAME.equals(name)) {
                final com.linkedin.datajob.DataJobInfo gmsDataJobInfo = new com.linkedin.datajob.DataJobInfo(data);
                result.setInfo(mapDataJobInfo(gmsDataJobInfo));
                result.setProperties(mapDataJobInfoToProperties(gmsDataJobInfo));
            } else if (DATA_JOB_INPUT_OUTPUT_ASPECT_NAME.equals(name)) {
                final com.linkedin.datajob.DataJobInputOutput gmsDataJobInputOutput = new com.linkedin.datajob.DataJobInputOutput(data);
                result.setInputOutput(mapDataJobInputOutput(gmsDataJobInputOutput));
            } else if (EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME.equals(name)) {
                final EditableDataJobProperties editableDataJobProperties = new EditableDataJobProperties(data);
                final DataJobEditableProperties dataJobEditableProperties = new DataJobEditableProperties();
                dataJobEditableProperties.setDescription(editableDataJobProperties.getDescription());
                result.setEditableProperties(dataJobEditableProperties);
            } else if (OWNERSHIP_ASPECT_NAME.equals(name)) {
                result.setOwnership(OwnershipMapper.map(new Ownership(data)));
            } else if (STATUS_ASPECT_NAME.equals(name)) {
                result.setStatus(StatusMapper.map(new Status(data)));
            } else if (GLOBAL_TAGS_ASPECT_NAME.equals(name)) {
                com.linkedin.datahub.graphql.generated.GlobalTags globalTags = GlobalTagsMapper.map(new GlobalTags(data));
                result.setGlobalTags(globalTags);
                result.setTags(globalTags);
            } else if (INSTITUTIONAL_MEMORY_ASPECT_NAME.equals(name)) {
                result.setInstitutionalMemory(InstitutionalMemoryMapper.map(new InstitutionalMemory(data)));
            } else if (GLOSSARY_TERMS_ASPECT_NAME.equals(name)) {
                result.setGlossaryTerms(GlossaryTermsMapper.map(new GlossaryTerms(data)));
            } else if (DOMAINS_ASPECT_NAME.equals(name)) {
                final Domains domains = new Domains(data);
                // Currently we only take the first domain if it exists.
                if (domains.getDomains().size() > 0) {
                    result.setDomain(Domain.builder()
                        .setType(EntityType.DOMAIN)
                        .setUrn(domains.getDomains().get(0).toString()).build());
                }
            } else if (DEPRECATION_ASPECT_NAME.equals(name)) {
                result.setDeprecation(DeprecationMapper.map(new Deprecation(data)));
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
