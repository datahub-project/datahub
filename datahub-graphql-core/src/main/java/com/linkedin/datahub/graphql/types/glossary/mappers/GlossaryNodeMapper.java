package com.linkedin.datahub.graphql.types.glossary.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.GlossaryNode;
import com.linkedin.datahub.graphql.generated.GlossaryNodeProperties;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.metadata.key.GlossaryNodeKey;

import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;

public class GlossaryNodeMapper implements ModelMapper<EntityResponse, GlossaryNode> {

  public static final GlossaryNodeMapper INSTANCE = new GlossaryNodeMapper();

  public static GlossaryNode map(@Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(entityResponse);
  }

  @Override
  public GlossaryNode apply(@Nonnull final EntityResponse entityResponse) {
    GlossaryNode result = new GlossaryNode();
    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.GLOSSARY_NODE);

    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<GlossaryNode> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(GLOSSARY_NODE_INFO_ASPECT_NAME, (glossaryNode, dataMap) ->
        glossaryNode.setProperties(mapGlossaryNodeProperties(dataMap)));
    mappingHelper.mapToResult(GLOSSARY_NODE_KEY_ASPECT_NAME, this::mapGlossaryNodeKey);
    mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (glossaryNode, dataMap) ->
        glossaryNode.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));

    return mappingHelper.getResult();
  }

  private GlossaryNodeProperties mapGlossaryNodeProperties(@Nonnull DataMap dataMap) {
    GlossaryNodeInfo glossaryNodeInfo = new GlossaryNodeInfo(dataMap);
    GlossaryNodeProperties result = new GlossaryNodeProperties();
    result.setDescription(glossaryNodeInfo.getDefinition());
    if (glossaryNodeInfo.hasName()) {
      result.setName(glossaryNodeInfo.getName());
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
