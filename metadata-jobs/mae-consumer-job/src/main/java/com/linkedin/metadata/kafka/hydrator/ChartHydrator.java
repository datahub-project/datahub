package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.ChartAspect;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.snapshot.Snapshot;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public class ChartHydrator extends Hydrator<ChartKey> {

  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  @Override
  protected void hydrateFromKey(ObjectNode document, ChartKey key) {
    document.put(DASHBOARD_TOOL, key.getDashboardTool());
  }

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot) {
    if (!snapshot.isChartSnapshot()) {
      log.error("Hydrator {} does not match type of snapshot {}", this.getClass().getSimpleName(),
          snapshot.getClass().getSimpleName());
    }
    for (ChartAspect aspect : snapshot.getChartSnapshot().getAspects()) {
      if (aspect.isChartInfo()) {
        document.put(TITLE, aspect.getChartInfo().getTitle());
      }
    }
  }
}
