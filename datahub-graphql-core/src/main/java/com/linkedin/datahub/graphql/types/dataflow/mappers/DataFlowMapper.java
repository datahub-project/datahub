package com.linkedin.datahub.graphql.types.dataflow.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.Deprecation;
import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.InstitutionalMemory;
import com.linkedin.common.Ownership;
import com.linkedin.common.Status;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.Container;
import com.linkedin.datahub.graphql.generated.DataFlow;
import com.linkedin.datahub.graphql.generated.DataFlowEditableProperties;
import com.linkedin.datahub.graphql.generated.DataFlowInfo;
import com.linkedin.datahub.graphql.generated.DataFlowProperties;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.BrowsePathsV2Mapper;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DataPlatformInstanceAspectMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DeprecationMapper;
import com.linkedin.datahub.graphql.types.common.mappers.InstitutionalMemoryMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StatusMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.common.mappers.util.SystemMetadataUtils;
import com.linkedin.datahub.graphql.types.domain.DomainAssociationMapper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.glossary.mappers.GlossaryTermsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.datajob.EditableDataFlowProperties;
import com.linkedin.domain.Domains;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataPlatformKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataFlowMapper implements ModelMapper<EntityResponse, DataFlow> {

  public static final DataFlowMapper INSTANCE = new DataFlowMapper();

  public static DataFlow map(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public DataFlow apply(
      @Nullable final QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final DataFlow result = new DataFlow();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.DATA_FLOW);
    Urn entityUrn = entityResponse.getUrn();

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    Long lastIngested = SystemMetadataUtils.getLastIngestedTime(aspectMap);
    result.setLastIngested(lastIngested);

    MappingHelper<DataFlow> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(DATA_FLOW_KEY_ASPECT_NAME, this::mapKey);
    mappingHelper.mapToResult(
        DATA_FLOW_INFO_ASPECT_NAME, (entity, dataMap) -> this.mapInfo(entity, dataMap, entityUrn));
    mappingHelper.mapToResult(
        EDITABLE_DATA_FLOW_PROPERTIES_ASPECT_NAME, this::mapEditableProperties);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (dataFlow, dataMap) ->
            dataFlow.setOwnership(OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        STATUS_ASPECT_NAME,
        (dataFlow, dataMap) -> dataFlow.setStatus(StatusMapper.map(context, new Status(dataMap))));
    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (dataFlow, dataMap) -> mapGlobalTags(context, dataFlow, dataMap, entityUrn));
    mappingHelper.mapToResult(
        INSTITUTIONAL_MEMORY_ASPECT_NAME,
        (dataFlow, dataMap) ->
            dataFlow.setInstitutionalMemory(
                InstitutionalMemoryMapper.map(
                    context, new InstitutionalMemory(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        GLOSSARY_TERMS_ASPECT_NAME,
        (dataFlow, dataMap) ->
            dataFlow.setGlossaryTerms(
                GlossaryTermsMapper.map(context, new GlossaryTerms(dataMap), entityUrn)));
    mappingHelper.mapToResult(context, DOMAINS_ASPECT_NAME, DataFlowMapper::mapDomains);
    mappingHelper.mapToResult(
        DEPRECATION_ASPECT_NAME,
        (dataFlow, dataMap) ->
            dataFlow.setDeprecation(DeprecationMapper.map(context, new Deprecation(dataMap))));
    mappingHelper.mapToResult(
        DATA_PLATFORM_INSTANCE_ASPECT_NAME,
        (dataset, dataMap) ->
            dataset.setDataPlatformInstance(
                DataPlatformInstanceAspectMapper.map(context, new DataPlatformInstance(dataMap))));
    mappingHelper.mapToResult(context, CONTAINER_ASPECT_NAME, DataFlowMapper::mapContainers);
    mappingHelper.mapToResult(
        BROWSE_PATHS_V2_ASPECT_NAME,
        (dataFlow, dataMap) ->
            dataFlow.setBrowsePathV2(BrowsePathsV2Mapper.map(context, new BrowsePathsV2(dataMap))));
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(mappingHelper.getResult(), DataFlow.class);
    } else {
      return mappingHelper.getResult();
    }
  }

  private void mapKey(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
    final DataFlowKey gmsKey = new DataFlowKey(dataMap);
    dataFlow.setOrchestrator(gmsKey.getOrchestrator());
    dataFlow.setFlowId(gmsKey.getFlowId());
    dataFlow.setCluster(gmsKey.getCluster());
    dataFlow.setPlatform(
        DataPlatform.builder()
            .setType(EntityType.DATA_PLATFORM)
            .setUrn(
                EntityKeyUtils.convertEntityKeyToUrn(
                        new DataPlatformKey().setPlatformName(gmsKey.getOrchestrator()),
                        DATA_PLATFORM_ENTITY_NAME)
                    .toString())
            .build());
  }

  private void mapInfo(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap, Urn entityUrn) {
    final com.linkedin.datajob.DataFlowInfo gmsDataFlowInfo =
        new com.linkedin.datajob.DataFlowInfo(dataMap);
    dataFlow.setInfo(mapDataFlowInfo(gmsDataFlowInfo, entityUrn));
    dataFlow.setProperties(mapDataFlowInfoToProperties(gmsDataFlowInfo, entityUrn));
  }

  /**
   * Maps GMS {@link com.linkedin.datajob.DataFlowInfo} to deprecated GraphQL {@link DataFlowInfo}
   */
  private DataFlowInfo mapDataFlowInfo(
      final com.linkedin.datajob.DataFlowInfo info, Urn entityUrn) {
    final DataFlowInfo result = new DataFlowInfo();
    result.setName(info.getName());
    result.setDescription(info.getDescription());
    result.setProject(info.getProject());
    if (info.hasExternalUrl()) {
      result.setExternalUrl(info.getExternalUrl().toString());
    }
    if (info.hasCustomProperties()) {
      result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
    }
    return result;
  }

  /**
   * Maps GMS {@link com.linkedin.datajob.DataFlowInfo} to new GraphQL {@link DataFlowProperties}
   */
  private DataFlowProperties mapDataFlowInfoToProperties(
      final com.linkedin.datajob.DataFlowInfo info, Urn entityUrn) {
    final DataFlowProperties result = new DataFlowProperties();
    result.setName(info.getName());
    result.setDescription(info.getDescription());
    result.setProject(info.getProject());
    if (info.hasExternalUrl()) {
      result.setExternalUrl(info.getExternalUrl().toString());
    }
    if (info.hasCustomProperties()) {
      result.setCustomProperties(CustomPropertiesMapper.map(info.getCustomProperties(), entityUrn));
    }
    return result;
  }

  private void mapEditableProperties(@Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
    final EditableDataFlowProperties editableDataFlowProperties =
        new EditableDataFlowProperties(dataMap);
    final DataFlowEditableProperties dataFlowEditableProperties = new DataFlowEditableProperties();
    dataFlowEditableProperties.setDescription(editableDataFlowProperties.getDescription());
    dataFlow.setEditableProperties(dataFlowEditableProperties);
  }

  private static void mapGlobalTags(
      @Nullable final QueryContext context,
      @Nonnull DataFlow dataFlow,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    com.linkedin.datahub.graphql.generated.GlobalTags globalTags =
        GlobalTagsMapper.map(context, new GlobalTags(dataMap), entityUrn);
    dataFlow.setGlobalTags(globalTags);
    dataFlow.setTags(globalTags);
  }

  private static void mapContainers(
      @Nullable final QueryContext context, @Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
    final com.linkedin.container.Container gmsContainer =
        new com.linkedin.container.Container(dataMap);
    dataFlow.setContainer(
        Container.builder()
            .setType(EntityType.CONTAINER)
            .setUrn(gmsContainer.getContainer().toString())
            .build());
  }

  private static void mapDomains(
      @Nullable final QueryContext context, @Nonnull DataFlow dataFlow, @Nonnull DataMap dataMap) {
    final Domains domains = new Domains(dataMap);
    // Currently we only take the first domain if it exists.
    dataFlow.setDomain(DomainAssociationMapper.map(context, domains, dataFlow.getUrn()));
  }
}
