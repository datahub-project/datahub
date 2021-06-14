package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.aspect.DashboardAspect;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.snapshot.Snapshot;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;


@Slf4j
@SuperBuilder
public class DashboardHydrator extends Hydrator<DashboardKey> {
  private static final String DASHBOARD_TOOL = "dashboardTool";
  private static final String TITLE = "title";

  @Override
  protected void hydrateFromKey(ObjectNode document, DashboardKey key) {
    document.put(DASHBOARD_TOOL, key.getDashboardTool());
  }

  @Override
  protected void hydrateFromSnapshot(ObjectNode document, Snapshot snapshot) {
    if (!snapshot.isDashboardSnapshot()) {
      log.error("Hydrator {} does not match type of snapshot {}", this.getClass().getSimpleName(),
          snapshot.getClass().getSimpleName());
    }
    for (DashboardAspect aspect : snapshot.getDashboardSnapshot().getAspects()) {
      if (aspect.isDashboardInfo()) {
        document.put(TITLE, aspect.getDashboardInfo().getTitle());
      }
    }
  }
}
