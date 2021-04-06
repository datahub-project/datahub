package com.linkedin.metadata.testing;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataProcessUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.common.urn.TagUrn;

import javax.annotation.Nonnull;


/**
 * Utilities related to URNs for testing.
 */
public final class Urns {
  private Urns() {
  }

  @Nonnull
  public static CorpuserUrn makeCorpUserUrn(@Nonnull String name) {
    return new CorpuserUrn(name);
  }

  @Nonnull
  public static CorpGroupUrn makeCorpGroupUrn(@Nonnull String name) {
    return new CorpGroupUrn(name);
  }

  @Nonnull
  public static DatasetUrn makeDatasetUrn(@Nonnull String name) {
    return new DatasetUrn(new DataPlatformUrn("mysql"), name, FabricType.DEV);
  }

  @Nonnull
  public static DatasetUrn makeDatasetUrn(@Nonnull String platform, @Nonnull String name, @Nonnull FabricType fabricType) {
    return new DatasetUrn(new DataPlatformUrn(platform), name, fabricType);
  }

  @Nonnull
  public static DataProcessUrn makeDataProcessUrn(@Nonnull String name) {
    return new DataProcessUrn("Azure Data Factory", name, FabricType.DEV);
  }

  @Nonnull
  public static ChartUrn makeChartUrn(@Nonnull String chartId) {
    return new ChartUrn("looker", chartId);
  }

  @Nonnull
  public static DashboardUrn makeDashboardUrn(@Nonnull String dashboardId) {
    return new DashboardUrn("looker", dashboardId);
  }

  @Nonnull
  public static MLModelUrn makeMLModelUrn(@Nonnull String name) {
    return new MLModelUrn(new DataPlatformUrn("mysql"), name, FabricType.DEV);
  }

  @Nonnull
  public static DataFlowUrn makeDataFlowUrn(@Nonnull String name) {
    return new DataFlowUrn("airflow", name, "production_cluster");
  }


  @Nonnull
  public static DataJobUrn makeDataJobUrn(@Nonnull String jobId) {
    return new DataJobUrn(new DataFlowUrn("airflow", "my_flow", "production_cluster"), jobId);
  }

  @Nonnull
  public static TagUrn makeTagUrn(@Nonnull String tagName) {
    return new TagUrn(tagName);
  }

}
