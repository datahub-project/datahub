package com.linkedin.metadata.search.utils;

import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.key.ChartKey;
import com.linkedin.metadata.key.DashboardKey;
import com.linkedin.metadata.key.DataFlowKey;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.net.URISyntaxException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrowsePathUtilsTest {

  private final EntityRegistry registry = new TestEntityRegistry();

  @Test
  public void testGetDefaultBrowsePath() throws URISyntaxException {

    // Datasets
    DatasetKey datasetKey =
        new DatasetKey()
            .setName("Test.A.B")
            .setOrigin(FabricType.PROD)
            .setPlatform(Urn.createFromString("urn:li:dataPlatform:kafka"));
    Urn datasetUrn = EntityKeyUtils.convertEntityKeyToUrn(datasetKey, "dataset");
    String datasetPath = BrowsePathUtils.getDefaultBrowsePath(datasetUrn, this.registry, '.');
    Assert.assertEquals(datasetPath, "/prod/kafka/test/a");

    // Charts
    ChartKey chartKey = new ChartKey().setChartId("Test/A/B").setDashboardTool("looker");
    Urn chartUrn = EntityKeyUtils.convertEntityKeyToUrn(chartKey, "chart");
    String chartPath = BrowsePathUtils.getDefaultBrowsePath(chartUrn, this.registry, '/');
    Assert.assertEquals(chartPath, "/looker");

    // Dashboards
    DashboardKey dashboardKey =
        new DashboardKey().setDashboardId("Test/A/B").setDashboardTool("looker");
    Urn dashboardUrn = EntityKeyUtils.convertEntityKeyToUrn(dashboardKey, "dashboard");
    String dashboardPath = BrowsePathUtils.getDefaultBrowsePath(dashboardUrn, this.registry, '/');
    Assert.assertEquals(dashboardPath, "/looker");

    // Data Flows
    DataFlowKey dataFlowKey =
        new DataFlowKey().setCluster("test").setFlowId("Test/A/B").setOrchestrator("airflow");
    Urn dataFlowUrn = EntityKeyUtils.convertEntityKeyToUrn(dataFlowKey, "dataFlow");
    String dataFlowPath = BrowsePathUtils.getDefaultBrowsePath(dataFlowUrn, this.registry, '/');
    Assert.assertEquals(dataFlowPath, "/airflow/test");

    // Data Jobs
    DataJobKey dataJobKey =
        new DataJobKey()
            .setFlow(Urn.createFromString("urn:li:dataFlow:(airflow,Test/A/B,test)"))
            .setJobId("Job/A/B");
    Urn dataJobUrn = EntityKeyUtils.convertEntityKeyToUrn(dataJobKey, "dataJob");
    String dataJobPath = BrowsePathUtils.getDefaultBrowsePath(dataJobUrn, this.registry, '/');
    Assert.assertEquals(dataJobPath, "/airflow/test");
  }

  @Test
  public void testBuildDataPlatformUrn() throws URISyntaxException {
    // Datasets
    DatasetKey datasetKey =
        new DatasetKey()
            .setName("Test.A.B")
            .setOrigin(FabricType.PROD)
            .setPlatform(Urn.createFromString("urn:li:dataPlatform:kafka"));
    Urn datasetUrn = EntityKeyUtils.convertEntityKeyToUrn(datasetKey, "dataset");
    Urn dataPlatformUrn1 = BrowsePathUtils.buildDataPlatformUrn(datasetUrn, this.registry);
    Assert.assertEquals(dataPlatformUrn1, Urn.createFromString("urn:li:dataPlatform:kafka"));

    // Charts
    ChartKey chartKey = new ChartKey().setChartId("Test/A/B").setDashboardTool("looker");
    Urn chartUrn = EntityKeyUtils.convertEntityKeyToUrn(chartKey, "chart");
    Urn dataPlatformUrn2 = BrowsePathUtils.buildDataPlatformUrn(chartUrn, this.registry);
    Assert.assertEquals(dataPlatformUrn2, Urn.createFromString("urn:li:dataPlatform:looker"));

    // Dashboards
    DashboardKey dashboardKey =
        new DashboardKey().setDashboardId("Test/A/B").setDashboardTool("looker");
    Urn dashboardUrn = EntityKeyUtils.convertEntityKeyToUrn(dashboardKey, "dashboard");
    Urn dataPlatformUrn3 = BrowsePathUtils.buildDataPlatformUrn(dashboardUrn, this.registry);
    Assert.assertEquals(dataPlatformUrn3, Urn.createFromString("urn:li:dataPlatform:looker"));

    // Data Flows
    DataFlowKey dataFlowKey =
        new DataFlowKey().setCluster("test").setFlowId("Test/A/B").setOrchestrator("airflow");
    Urn dataFlowUrn = EntityKeyUtils.convertEntityKeyToUrn(dataFlowKey, "dataFlow");
    Urn dataPlatformUrn4 = BrowsePathUtils.buildDataPlatformUrn(dataFlowUrn, this.registry);
    Assert.assertEquals(dataPlatformUrn4, Urn.createFromString("urn:li:dataPlatform:airflow"));

    // Data Jobs
    DataJobKey dataJobKey =
        new DataJobKey()
            .setFlow(Urn.createFromString("urn:li:dataFlow:(airflow,Test/A/B,test)"))
            .setJobId("Job/A/B");
    Urn dataJobUrn = EntityKeyUtils.convertEntityKeyToUrn(dataJobKey, "dataJob");
    Urn dataPlatformUrn5 = BrowsePathUtils.buildDataPlatformUrn(dataJobUrn, this.registry);
    Assert.assertEquals(dataPlatformUrn5, Urn.createFromString("urn:li:dataPlatform:airflow"));
  }
}
