package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class DashboardHydrator extends BaseHydrator<DashboardSnapshot> {
  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, DashboardSnapshot snapshot) {
    for (DashboardAspect aspect : snapshot.getAspects()) {
      if (aspect.isDashboardInfo()) {
        document.put(TITLE, aspect.getDashboardInfo().getTitle());
      } else if (aspect.isDashboardKey()) {
        document.put(DASHBOARD_TOOL, aspect.getDashboardKey().getDashboardTool());
      }
    }
  }
}
