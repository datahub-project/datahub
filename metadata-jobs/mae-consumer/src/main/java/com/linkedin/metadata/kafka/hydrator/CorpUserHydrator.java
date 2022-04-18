package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.key.CorpUserKey;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
public class CorpUserHydrator extends BaseHydrator {

  private static final String USER_NAME = "username";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(CORP_USER_INFO_ASPECT_NAME, (jsonNodes, dataMap) ->
        jsonNodes.put(NAME, new CorpUserInfo(dataMap).getDisplayName()));
    mappingHelper.mapToResult(CORP_USER_KEY_ASPECT_NAME, (jsonNodes, dataMap) ->
        jsonNodes.put(USER_NAME, new CorpUserKey(dataMap).getUsername()));
  }
}
