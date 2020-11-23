package com.linkedin.metadata.builders.common;

import com.linkedin.chart.ChartDataSourceType;
import com.linkedin.chart.ChartDataSourceTypeArray;
import com.linkedin.chart.ChartInfo;
import com.linkedin.chart.ChartQuery;
import com.linkedin.chart.ChartQueryType;
import com.linkedin.chart.ChartType;
import com.linkedin.common.AccessLevel;
import com.linkedin.common.ChangeAuditStamps;
import com.linkedin.common.Status;
import com.linkedin.metadata.aspect.ChartAspect;
import java.util.Collections;

import static com.linkedin.metadata.testing.Owners.*;
import static com.linkedin.metadata.testing.Urns.*;
import static com.linkedin.metadata.utils.AuditStamps.*;


public class ChartTestUtils {

  private ChartTestUtils() {
    // Util class should not have public constructor
  }

  public static ChartInfo makeChartInfo() {
    return new ChartInfo()
        .setTitle("FooChart")
        .setDescription("Descripton for FooChart")
        .setLastModified(new ChangeAuditStamps()
            .setCreated(makeAuditStamp("fooUser"))
            .setLastModified(makeAuditStamp("fooUser"))
        )
        .setInputs(new ChartDataSourceTypeArray(Collections.singleton(ChartDataSourceType.create(makeDatasetUrn("fooDataset")))))
        .setType(ChartType.PIE)
        .setAccess(AccessLevel.PUBLIC);
  }

  public static ChartAspect makeChartInfoAspect() {
    ChartAspect aspect = new ChartAspect();
    aspect.setChartInfo(makeChartInfo());
    return aspect;
  }

  public static ChartAspect makeChartQueryAspect() {
    ChartAspect aspect = new ChartAspect();
    aspect.setChartQuery(new ChartQuery()
        .setRawQuery("fooQuery")
        .setType(ChartQueryType.LOOKML));
    return aspect;
  }

  public static ChartAspect makeOwnershipAspect() {
    ChartAspect aspect = new ChartAspect();
    aspect.setOwnership(makeOwnership("fooUser"));
    return aspect;
  }

  public static ChartAspect makeStatusAspect() {
    ChartAspect aspect = new ChartAspect();
    aspect.setStatus(new Status().setRemoved(true));
    return aspect;
  }
}