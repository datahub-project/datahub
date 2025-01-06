package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.Forms;
import com.linkedin.common.Origin;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpGroupMapper implements ModelMapper<EntityResponse, CorpGroup> {

  public static final CorpGroupMapper INSTANCE = new CorpGroupMapper();

  public static CorpGroup map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public CorpGroup apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    final CorpGroup result = new CorpGroup();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.CORP_GROUP);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<CorpGroup> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(CORP_GROUP_KEY_ASPECT_NAME, this::mapCorpGroupKey);
    mappingHelper.mapToResult(context, CORP_GROUP_INFO_ASPECT_NAME, this::mapCorpGroupInfo);
    mappingHelper.mapToResult(
        context, CORP_GROUP_EDITABLE_INFO_ASPECT_NAME, this::mapCorpGroupEditableInfo);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (entity, dataMap) -> this.mapOwnership(context, entity, dataMap, entityUrn));
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
    if (aspectMap.containsKey(ORIGIN_ASPECT_NAME)) {
      mappingHelper.mapToResult(ORIGIN_ASPECT_NAME, this::mapEntityOriginType);
    } else {
      com.linkedin.datahub.graphql.generated.Origin mappedGroupOrigin =
          new com.linkedin.datahub.graphql.generated.Origin();
      mappedGroupOrigin.setType(com.linkedin.datahub.graphql.generated.OriginType.UNKNOWN);
      result.setOrigin(mappedGroupOrigin);
    }
    return mappingHelper.getResult();
  }

  private void mapCorpGroupKey(@Nonnull CorpGroup corpGroup, @Nonnull DataMap dataMap) {
    CorpGroupKey corpGroupKey = new CorpGroupKey(dataMap);
    corpGroup.setName(corpGroupKey.getName());
  }

  private void mapCorpGroupInfo(
      @Nullable QueryContext context, @Nonnull CorpGroup corpGroup, @Nonnull DataMap dataMap) {
    CorpGroupInfo corpGroupInfo = new CorpGroupInfo(dataMap);
    corpGroup.setProperties(CorpGroupPropertiesMapper.map(context, corpGroupInfo));
    corpGroup.setInfo(CorpGroupInfoMapper.map(context, corpGroupInfo));
  }

  private void mapCorpGroupEditableInfo(
      @Nullable QueryContext context, @Nonnull CorpGroup corpGroup, @Nonnull DataMap dataMap) {
    corpGroup.setEditableProperties(
        CorpGroupEditablePropertiesMapper.map(context, new CorpGroupEditableInfo(dataMap)));
  }

  private void mapOwnership(
      @Nullable QueryContext context,
      @Nonnull CorpGroup corpGroup,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    corpGroup.setOwnership(OwnershipMapper.map(context, new Ownership(dataMap), entityUrn));
  }

  private void mapEntityOriginType(@Nonnull CorpGroup corpGroup, @Nonnull DataMap dataMap) {
    Origin groupOrigin = new Origin(dataMap);
    com.linkedin.datahub.graphql.generated.Origin mappedGroupOrigin =
        new com.linkedin.datahub.graphql.generated.Origin();
    if (groupOrigin.hasType()) {
      mappedGroupOrigin.setType(
          com.linkedin.datahub.graphql.generated.OriginType.valueOf(
              groupOrigin.getType().toString()));
    } else {
      mappedGroupOrigin.setType(com.linkedin.datahub.graphql.generated.OriginType.UNKNOWN);
    }
    if (groupOrigin.hasExternalType()) {
      mappedGroupOrigin.setExternalType(groupOrigin.getExternalType());
    }
    corpGroup.setOrigin(mappedGroupOrigin);
  }
}
