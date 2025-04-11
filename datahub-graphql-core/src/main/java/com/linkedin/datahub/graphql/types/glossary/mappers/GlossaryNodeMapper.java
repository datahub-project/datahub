package com.linkedin.datahub.graphql.types.glossary.mappers;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.canView;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.DisplayProperties;
import com.linkedin.common.Forms;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryNodeProperties;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.DisplayPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.key.GlossaryNodeKey;
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class GlossaryNodeMapper implements ModelMapper<EntityResponse, GlossaryNode> {

  public static final GlossaryNodeMapper INSTANCE = new GlossaryNodeMapper();

  public static GlossaryNode map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse);
  }

  @Override
  public GlossaryNode apply(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    GlossaryNode result = new GlossaryNode();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.GLOSSARY_NODE);
    Urn entityUrn = entityResponse.getUrn();

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<GlossaryNode> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(
        GLOSSARY_NODE_INFO_ASPECT_NAME,
        (glossaryNode, dataMap) ->
            glossaryNode.setProperties(mapGlossaryNodeProperties(dataMap, entityUrn)));
    mappingHelper.mapToResult(GLOSSARY_NODE_KEY_ASPECT_NAME, this::mapGlossaryNodeKey);
    mappingHelper.mapToResult(
        OWNERSHIP_ASPECT_NAME,
        (glossaryNode, dataMap) ->
            glossaryNode.setOwnership(
                OwnershipMapper.map(context, new Ownership(dataMap), entityUrn)));
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
    mappingHelper.mapToResult(
        DISPLAY_PROPERTIES_ASPECT_NAME,
        ((glossaryNode, dataMap) ->
            glossaryNode.setDisplayProperties(
                DisplayPropertiesMapper.map(context, new DisplayProperties(dataMap)))));

    if (context != null && !canView(context.getOperationContext(), entityUrn)) {
      return AuthorizationUtils.restrictEntity(mappingHelper.getResult(), GlossaryNode.class);
    } else {
      return mappingHelper.getResult();
    }
  }

  private GlossaryNodeProperties mapGlossaryNodeProperties(
      @Nonnull DataMap dataMap, @Nonnull final Urn entityUrn) {
    GlossaryNodeInfo glossaryNodeInfo = new GlossaryNodeInfo(dataMap);
    GlossaryNodeProperties result = new GlossaryNodeProperties();
    result.setDescription(glossaryNodeInfo.getDefinition());
    if (glossaryNodeInfo.hasName()) {
      result.setName(glossaryNodeInfo.getName());
    }
    if (glossaryNodeInfo.hasCustomProperties()) {
      result.setCustomProperties(
          CustomPropertiesMapper.map(glossaryNodeInfo.getCustomProperties(), entityUrn));
    }
    return result;
  }

  private void mapGlossaryNodeKey(@Nonnull GlossaryNode glossaryNode, @Nonnull DataMap dataMap) {
    GlossaryNodeKey glossaryNodeKey = new GlossaryNodeKey(dataMap);

    if (glossaryNode.getProperties() == null) {
      glossaryNode.setProperties(new GlossaryNodeProperties());
    }

    if (glossaryNode.getProperties().getName() == null) {
      glossaryNode.getProperties().setName(glossaryNodeKey.getName());
    }
  }
}
