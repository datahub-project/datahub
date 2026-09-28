package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.DataPlatformInstanceUrn;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import java.net.URISyntaxException;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrowsePathV2UtilsTest {

  private static final String DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test.a.b,DEV)";
  private static final String CHART_URN = "urn:li:chart:(looker,baz)";
  private static final String DASHBOARD_URN = "urn:li:dashboard:(airflow,id)";
  private static final String DATA_FLOW_URN = "urn:li:dataFlow:(orchestrator,flowId,cluster)";
  private static final String CONTAINER_URN1 = "urn:li:container:test-container1";
  private static final String CONTAINER_URN2 = "urn:li:container:test-container2";

  private final EntityRegistry registry = new TestEntityRegistry();

  @Test
  public void testGetDefaultDatasetBrowsePathV2WithContainers() throws URISyntaxException {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);
    final Urn containerUrn1 = UrnUtils.getUrn(CONTAINER_URN1);
    final Urn containerUrn2 = UrnUtils.getUrn(CONTAINER_URN2);
    OperationContext opContext =
        mockOpContextWithContainerParents(datasetUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, datasetUrn, this.registry, '.', true);
    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    BrowsePathEntry entry1 =
        new BrowsePathEntry().setId(containerUrn1.toString()).setUrn(containerUrn1);
    BrowsePathEntry entry2 =
        new BrowsePathEntry().setId(containerUrn2.toString()).setUrn(containerUrn2);
    expectedPath.add(entry2);
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  @Test
  public void testGetDefaultDatasetBrowsePathV2WithContainersFlagOff() throws URISyntaxException {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mockOpContext(), datasetUrn, this.registry, '.', false);
    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    BrowsePathEntry entry1 = new BrowsePathEntry().setId("test");
    BrowsePathEntry entry2 = new BrowsePathEntry().setId("a");
    expectedPath.add(entry1);
    expectedPath.add(entry2);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  @Test
  public void testGetDefaultChartBrowsePathV2WithContainers() throws URISyntaxException {
    Urn chartUrn = UrnUtils.getUrn(CHART_URN);
    final Urn containerUrn1 = UrnUtils.getUrn(CONTAINER_URN1);
    final Urn containerUrn2 = UrnUtils.getUrn(CONTAINER_URN2);
    OperationContext opContext =
        mockOpContextWithContainerParents(chartUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, chartUrn, this.registry, '.', true);
    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    BrowsePathEntry entry1 =
        new BrowsePathEntry().setId(containerUrn1.toString()).setUrn(containerUrn1);
    BrowsePathEntry entry2 =
        new BrowsePathEntry().setId(containerUrn2.toString()).setUrn(containerUrn2);
    expectedPath.add(entry2);
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  @Test
  public void testGetDefaultDashboardBrowsePathV2WithContainers() throws URISyntaxException {
    Urn dashboardUrn = UrnUtils.getUrn(DASHBOARD_URN);
    final Urn containerUrn1 = UrnUtils.getUrn(CONTAINER_URN1);
    final Urn containerUrn2 = UrnUtils.getUrn(CONTAINER_URN2);
    OperationContext opContext =
        mockOpContextWithContainerParents(dashboardUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, dashboardUrn, this.registry, '.', true);
    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    BrowsePathEntry entry1 =
        new BrowsePathEntry().setId(containerUrn1.toString()).setUrn(containerUrn1);
    BrowsePathEntry entry2 =
        new BrowsePathEntry().setId(containerUrn2.toString()).setUrn(containerUrn2);
    expectedPath.add(entry2);
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  @Test
  public void testGetDefaultBrowsePathV2WithoutContainers() throws URISyntaxException {
    OperationContext opContext = mockOpContext();

    // Datasets
    DatasetKey datasetKey =
        new DatasetKey()
            .setName("Test.A.B")
            .setOrigin(FabricType.PROD)
            .setPlatform(Urn.createFromString("urn:li:dataPlatform:kafka"));
    Urn datasetUrn = EntityKeyUtils.convertEntityKeyToUrn(datasetKey, "dataset");
    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, datasetUrn, this.registry, '.', true);
    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    BrowsePathEntry entry1 = new BrowsePathEntry().setId("Test");
    BrowsePathEntry entry2 = new BrowsePathEntry().setId("A");
    expectedPath.add(entry1);
    expectedPath.add(entry2);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Charts
    Urn chartUrn = UrnUtils.getUrn(CHART_URN);
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, chartUrn, this.registry, '/', true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId("Default");
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Dashboards
    Urn dashboardUrn = UrnUtils.getUrn(DASHBOARD_URN);
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, dashboardUrn, this.registry, '/', true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId("Default");
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Data Flows
    Urn dataFlowUrn = UrnUtils.getUrn(DATA_FLOW_URN);
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, dataFlowUrn, this.registry, '/', true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId("Default");
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Data Jobs
    DataJobKey dataJobKey = new DataJobKey().setFlow(dataFlowUrn).setJobId("Job/A/B");
    Urn dataJobUrn = EntityKeyUtils.convertEntityKeyToUrn(dataJobKey, "dataJob");
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(opContext, dataJobUrn, this.registry, '/', true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId(dataFlowUrn.toString()).setUrn(dataFlowUrn);
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  /**
   * When a Snowflake DataPlatformInstance entity already exists, the default BrowsePathsV2 should
   * use the URN-form entry for the platform instance segment (matching Python ingestion output).
   */
  @Test
  public void testGetDefaultDatasetBrowsePathV2WithPlatformInstance() throws URISyntaxException {
    String snowflakeDatasetUrn =
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,myinstance.DB.SCHEMA.TABLE,PROD)";
    Urn datasetUrn = UrnUtils.getUrn(snowflakeDatasetUrn);
    DataPlatformInstanceUrn platformInstanceUrn =
        new DataPlatformInstanceUrn(new DataPlatformUrn("snowflake"), "myinstance");

    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getLatestAspectObject(
            any(), eq(platformInstanceUrn), eq(DATA_PLATFORM_INSTANCE_KEY_ASPECT_NAME)))
        .thenReturn(new Aspect());

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mockOpContext(retriever), datasetUrn, this.registry, '.', false);

    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    expectedPath.add(
        new BrowsePathEntry().setId(platformInstanceUrn.toString()).setUrn(platformInstanceUrn));
    expectedPath.add(new BrowsePathEntry().setId("DB"));
    expectedPath.add(new BrowsePathEntry().setId("SCHEMA"));
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  /**
   * When the DataPlatformInstance entity does NOT exist yet (e.g. lineage runs before schema
   * ingestion), the plain-name fallback behavior is preserved unchanged.
   */
  @Test
  public void testGetDefaultDatasetBrowsePathV2WithoutPlatformInstance() throws URISyntaxException {
    String snowflakeDatasetUrn =
        "urn:li:dataset:(urn:li:dataPlatform:snowflake,myinstance.DB.SCHEMA.TABLE,PROD)";
    Urn datasetUrn = UrnUtils.getUrn(snowflakeDatasetUrn);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mockOpContext(), datasetUrn, this.registry, '.', false);

    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    expectedPath.add(new BrowsePathEntry().setId("myinstance"));
    expectedPath.add(new BrowsePathEntry().setId("DB"));
    expectedPath.add(new BrowsePathEntry().setId("SCHEMA"));
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  /** BigQuery datasets without a platform instance: missing key aspect, entries are unchanged. */
  @Test
  public void testGetDefaultDatasetBrowsePathV2BigQueryNoPlatformInstance()
      throws URISyntaxException {
    Urn datasetUrn = UrnUtils.getUrn(DATASET_URN);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mockOpContext(), datasetUrn, this.registry, '.', false);

    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    expectedPath.add(new BrowsePathEntry().setId("test"));
    expectedPath.add(new BrowsePathEntry().setId("a"));
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  private OperationContext mockOpContext() {
    return mockOpContext(mock(CachingAspectRetriever.class));
  }

  private OperationContext mockOpContext(CachingAspectRetriever retriever) {
    when(retriever.getLatestAspectObjects(any(), any(), any())).thenReturn(Map.of());
    OperationContext opContext = mock(OperationContext.class);
    RetrieverContext retrieverContext = mock(RetrieverContext.class);
    when(opContext.getRetrieverContext()).thenReturn(retrieverContext);
    when(retrieverContext.getCachingAspectRetriever()).thenReturn(retriever);
    return opContext;
  }

  private OperationContext mockOpContextWithContainerParents(
      Urn entityUrn, Urn containerUrn1, Urn containerUrn2) {
    CachingAspectRetriever retriever = mock(CachingAspectRetriever.class);
    when(retriever.getLatestAspectObject(any(), eq(entityUrn), eq(CONTAINER_ASPECT_NAME)))
        .thenReturn(new Aspect(new Container().setContainer(containerUrn1).data()));
    when(retriever.getLatestAspectObject(any(), eq(containerUrn1), eq(CONTAINER_ASPECT_NAME)))
        .thenReturn(new Aspect(new Container().setContainer(containerUrn2).data()));
    return mockOpContext(retriever);
  }
}
