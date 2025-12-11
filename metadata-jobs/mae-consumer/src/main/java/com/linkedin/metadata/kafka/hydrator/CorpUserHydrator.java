/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka.hydrator;

import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.metadata.key.CorpUserKey;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CorpUserHydrator extends BaseHydrator {

  private static final String USER_NAME = "username";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(
        CORP_USER_INFO_ASPECT_NAME,
        (jsonNodes, dataMap) -> jsonNodes.put(NAME, new CorpUserInfo(dataMap).getDisplayName()));
    mappingHelper.mapToResult(
        CORP_USER_KEY_ASPECT_NAME,
        (jsonNodes, dataMap) -> jsonNodes.put(USER_NAME, new CorpUserKey(dataMap).getUsername()));
  }
}
