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
import com.linkedin.chart.ChartInfo;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.ChartKey;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ChartHydrator extends BaseHydrator {

  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(
        CHART_INFO_ASPECT_NAME,
        (jsonNodes, dataMap) -> jsonNodes.put(TITLE, new ChartInfo(dataMap).getTitle()));
    mappingHelper.mapToResult(
        CHART_KEY_ASPECT_NAME,
        (jsonNodes, dataMap) ->
            jsonNodes.put(DASHBOARD_TOOL, new ChartKey(dataMap).getDashboardTool()));
  }
}
