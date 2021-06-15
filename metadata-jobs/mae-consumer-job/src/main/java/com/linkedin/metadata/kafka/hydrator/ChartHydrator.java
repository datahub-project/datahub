package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class ChartHydrator extends BaseHydrator<ChartSnapshot> {

  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, ChartSnapshot snapshot) {
    for (ChartAspect aspect : snapshot.getAspects()) {
      if (aspect.isChartInfo()) {
        document.put(TITLE, aspect.getChartInfo().getTitle());
      } else if (aspect.isChartKey()) {
        document.put(DASHBOARD_TOOL, aspect.getChartKey().getDashboardTool());
      }
    }
  }
}
