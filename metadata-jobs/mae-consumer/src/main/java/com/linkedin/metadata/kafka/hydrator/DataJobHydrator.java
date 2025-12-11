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
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataJobHydrator extends BaseHydrator {

  private static final String ORCHESTRATOR = "orchestrator";
  private static final String NAME = "name";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(
        DATA_JOB_INFO_ASPECT_NAME,
        (jsonNodes, dataMap) -> jsonNodes.put(NAME, new DataJobInfo(dataMap).getName()));
    try {
      mappingHelper.mapToResult(DATA_JOB_KEY_ASPECT_NAME, this::mapKey);
    } catch (Exception e) {
      log.info("Failed to parse data flow urn for dataJob: {}", entityResponse.getUrn());
    }
  }

  private void mapKey(ObjectNode jsonNodes, DataMap dataMap) {
    DataJobKey dataJobKey = new DataJobKey(dataMap);
    DataFlowKey dataFlowKey =
        (DataFlowKey)
            EntityKeyUtils.convertUrnToEntityKeyInternal(
                dataJobKey.getFlow(), new DataFlowKey().schema());
    jsonNodes.put(ORCHESTRATOR, dataFlowKey.getOrchestrator());
  }
}
