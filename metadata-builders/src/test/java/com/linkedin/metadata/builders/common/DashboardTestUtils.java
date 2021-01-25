package com.linkedin.metadata.builders.common;

import com.linkedin.common.AccessLevel;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.ChartUrnArray;
import com.linkedin.common.Status;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.metadata.aspect.DashboardAspect;

import static com.linkedin.metadata.testing.Owners.*;
import static com.linkedin.metadata.testing.Urns.*;
import static com.linkedin.metadata.utils.AuditStamps.*;


public class DashboardTestUtils {

  private DashboardTestUtils() {
    // Util class should not have public constructor
  }

  public static DashboardInfo makeDashboardInfo() {
    return new DashboardInfo()
        .setTitle("FooDashboard")
        .setDescription("Descripton for FooDashboard")
        .setLastModified(new ChangeAuditStamps()
            .setCreated(makeAuditStamp("fooUser"))
            .setLastModified(makeAuditStamp("fooUser"))
        )
        .setCharts(new ChartUrnArray(makeChartUrn("1")))
        .setAccess(AccessLevel.PUBLIC)
        .setLastRefreshed(0L);
  }

  public static DashboardAspect makeDashboardInfoAspect() {
    DashboardAspect aspect = new DashboardAspect();
    aspect.setDashboardInfo(makeDashboardInfo());
    return aspect;
  }

  public static DashboardAspect makeOwnershipAspect() {
    DashboardAspect aspect = new DashboardAspect();
    aspect.setOwnership(makeOwnership("fooUser"));
    return aspect;
  }

  public static DashboardAspect makeStatusAspect() {
    DashboardAspect aspect = new DashboardAspect();
    aspect.setStatus(new Status().setRemoved(true));
    return aspect;
  }
}