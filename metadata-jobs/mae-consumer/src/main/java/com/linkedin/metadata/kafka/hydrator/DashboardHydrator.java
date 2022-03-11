package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.DashboardKey;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
public class DashboardHydrator extends BaseHydrator {
  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  @Override
  protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
    mappingHelper.mapToResult(DASHBOARD_INFO_ASPECT_NAME, (jsonNodes, dataMap) ->
        jsonNodes.put(TITLE, new DashboardInfo(dataMap).getTitle()));
    mappingHelper.mapToResult(DASHBOARD_KEY_ASPECT_NAME, (jsonNodes, dataMap) ->
        jsonNodes.put(DASHBOARD_TOOL, new DashboardKey(dataMap).getDashboardTool()));
  }
}
