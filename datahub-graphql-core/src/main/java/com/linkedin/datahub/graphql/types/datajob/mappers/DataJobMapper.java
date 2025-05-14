package com.linkedin.datahub.graphql.types.datajob.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.*;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.DataJobEditableProperties;
import com.linkedin.datahub.graphql.generated.DataJobInfo;
import com.linkedin.datahub.graphql.generated.DataJobInputOutput;
import com.linkedin.datahub.graphql.generated.DataJobProperties;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.*;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datajob.EditableDataJobProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.structured.StructuredProperties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataJobMapper implements ModelMapper<EntityResponse, DataJob> {

  public static final DataJobMapper INSTANCE = new DataJobMapper();

  public static DataJob map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataJob apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataJob result = new DataJob();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_JOB);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    entityResponse
        .getAspects()
        .forEach(
            (name, aspect) -> {
              DataMap data = aspect.getValue().data();
              if (DATA_JOB_KEY_ASPECT_NAME.equals(name)) {
                final DataJobKey gmsKey = new DataJobKey(data);
                if (context == null || canView(context.getOperationContext(), gmsKey.getFlow())) {
                  result.setDataFlow(
                      new DataFlow.Builder().setUrn(gmsKey.getFlow().toString()).build());
                }
                result.setJobId(gmsKey.getJobId());
              } else if (DATA_JOB_INFO_ASPECT_NAME.equals(name)) {
                final com.linkedin.datajob.DataJobInfo gmsDataJobInfo =
                    new com.linkedin.datajob.DataJobInfo(data);
                result.setInfo(mapDataJobInfo(gmsDataJobInfo, entityUrn));
                result.setProperties(mapDataJobInfoToProperties(gmsDataJobInfo, entityUrn));
              } else if (DATA_JOB_INPUT_OUTPUT_ASPECT_NAME.equals(name)) {
                final com.linkedin.datajob.DataJobInputOutput gmsDataJobInputOutput =
                    new com.linkedin.datajob.DataJobInputOutput(data);
                result.setInputOutput(mapDataJobInputOutput(gmsDataJobInputOutput));
              } else if (EDITABLE_DATA_JOB_PROPERTIES_ASPECT_NAME.equals(name)) {
                final EditableDataJobProperties editableDataJobProperties =
                    new EditableDataJobProperties(data);
                final DataJobEditableProperties dataJobEditableProperties =
                    new DataJobEditableProperties();
                dataJobEditableProperties.setDescription(
                    editableDataJobProperties.getDescription());
                result.setEditableProperties(dataJobEditableProperties);
              } else if (OWNERSHIP_ASPECT_NAME.equals(name)) {
                result.setOwnership(OwnershipMapper.map(context, new Ownership(data), entityUrn));
              } else if (STATUS_ASPECT_NAME.equals(name)) {
                result.setStatus(StatusMapper.map(context, new Status(data)));
              } else if (GLOBAL_TAGS_ASPECT_NAME.equals(name)) {
                com.linkedin.datahub.graphql.generated.GlobalTags globalTags =
                    GlobalTagsMapper.map(context, new GlobalTags(data), entityUrn);
                result.setGlobalTags(globalTags);
                result.setTags(globalTags);
              } else if (INSTITUTIONAL_MEMORY_ASPECT_NAME.equals(name)) {
                result.setInstitutionalMemory(
                    InstitutionalMemoryMapper.map(
                        context, new InstitutionalMemory(data), entityUrn));
              } else if (GLOSSARY_TERMS_ASPECT_NAME.equals(name)) {
                result.setGlossaryTerms(
                    GlossaryTermsMapper.map(context, new GlossaryTerms(data), entityUrn));
              } else if (DOMAINS_ASPECT_NAME.equals(name)) {
                final Domains domains = new Domains(data);
                // Currently we only take the first domain if it exists.
                result.setDomain(
                    DomainAssociationMapper.map(context, domains, entityUrn.toString()));
              } else if (DEPRECATION_ASPECT_NAME.equals(name)) {
                result.setDeprecation(DeprecationMapper.map(context, new Deprecation(data)));
              } else if (DATA_PLATFORM_INSTANCE_ASPECT_NAME.equals(name)) {
                result.setDataPlatformInstance(
                    DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(data)));
              } else if (CONTAINER_ASPECT_NAME.equals(name)) {
                final com.linkedin.container.Container gmsContainer =
                    new com.linkedin.container.Container(data);
                result.setContainer(
                    Container.builder()
                        .setType(EntityType.CONTAINER)
                        .setUrn(gmsContainer.getContainer().toString())
                        .build());
              } else if (BROWSE_PATHS_V2_ASPECT_NAME.equals(name)) {
                result.setBrowsePathV2(BrowsePathsV2Mapper.map(context, new BrowsePathsV2(data)));
              } else if (SUB_TYPES_ASPECT_NAME.equals(name)) {
                result.setSubTypes(SubTypesMapper.map(context, new SubTypes(data)));
              } else if (STRUCTURED_PROPERTIES_ASPECT_NAME.equals(name)) {
                result.setStructuredProperties(
                    StructuredPropertiesMapper.map(
                        context, new StructuredProperties(data), entityUrn));
              } else if (FORMS_ASPECT_NAME.equals(name)) {
                result.setForms(FormsMapper.map(new Forms(data), entityUrn.toString()));
              } else if (DATA_TRANSFORM_LOGIC_ASPECT_NAME.equals(name)) {
                result.setDataTransformLogic(
                    DataTransformLogicMapper.map(context, new DataTransformLogic(data)));
              }
            });

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(result, DataJob.class);
    } else {
      return result;
    }
  }

  /** Maps GMS {@link com.linkedin.datajob.DataJobInfo} to deprecated GraphQL {@link DataJobInfo} */
  private DataJobInfo mapDataJobInfo(final com.linkedin.datajob.DataJobInfo info, Urn entityUrn) {
    final DataJobInfo result = new DataJobInfo();
    result.setName(info.getName());
    result.setDescription(info.getDescription());
    if (info.hasExternalUrl()) {
      result.setExternalUrl(info.getExternalUrl().toString());
    }
    if (info.hasCustomProperties()) {
      result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
    }
    return result;
  }

  /** Maps GMS {@link com.linkedin.datajob.DataJobInfo} to new GraphQL {@link DataJobProperties} */
  private DataJobProperties mapDataJobInfoToProperties(
      final com.linkedin.datajob.DataJobInfo info, Urn entityUrn) {
    final DataJobProperties result = new DataJobProperties();
    result.setName(info.getName());
    result.setDescription(info.getDescription());
    if (info.hasExternalUrl()) {
      result.setExternalUrl(info.getExternalUrl().toString());
    }
    if (info.hasCustomProperties()) {
      result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
    }
    return result;
  }

  private DataJobInputOutput mapDataJobInputOutput(
      final com.linkedin.datajob.DataJobInputOutput inputOutput) {
    final DataJobInputOutput result = new DataJobInputOutput();
    if (inputOutput.hasInputDatasets()) {
      result.setInputDatasets(
          inputOutput.getInputDatasets().stream()
              .map(
                  urn -> {
                    final Dataset dataset = new Dataset();
                    dataset.setUrn(urn.toString());
                    return dataset;
                  })
              .collect(Collectors.toList()));
    } else {
      result.setInputDatasets(ImmutableList.of());
    }
    if (inputOutput.hasOutputDatasets()) {
      result.setOutputDatasets(
          inputOutput.getOutputDatasets().stream()
              .map(
                  urn -> {
                    final Dataset dataset = new Dataset();
                    dataset.setUrn(urn.toString());
                    return dataset;
                  })
              .collect(Collectors.toList()));
    } else {
      result.setOutputDatasets(ImmutableList.of());
    }
    if (inputOutput.hasInputDatajobs()) {
      result.setInputDatajobs(
          inputOutput.getInputDatajobs().stream()
              .map(
                  urn -> {
                    final DataJob dataJob = new DataJob();
                    dataJob.setUrn(urn.toString());
                    return dataJob;
                  })
              .collect(Collectors.toList()));
    } else {
      result.setInputDatajobs(ImmutableList.of());
    }

    if (inputOutput.hasFineGrainedLineages() && inputOutput.getFineGrainedLineages() != null) {
      result.setFineGrainedLineages(
          FineGrainedLineagesMapper.map(inputOutput.getFineGrainedLineages()));
    }

    return result;
  }
}
