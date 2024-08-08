package com.linkedin.metadata.search.utils;

import static com.linkedin.metadata.Constants.CONTAINER_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.FabricType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.TestEntityRegistry;
import com.linkedin.metadata.key.DataJobKey;
import com.linkedin.metadata.key.DatasetKey;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
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
    EntityService mockService =
        initMockServiceWithContainerParents(datasetUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), datasetUrn, this.registry, '.', mockService, true);
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
    final Urn containerUrn1 = UrnUtils.getUrn(CONTAINER_URN1);
    final Urn containerUrn2 = UrnUtils.getUrn(CONTAINER_URN2);
    EntityService mockService =
        initMockServiceWithContainerParents(datasetUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), datasetUrn, this.registry, '.', mockService, false);
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
    EntityService mockService =
        initMockServiceWithContainerParents(chartUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), chartUrn, this.registry, '.', mockService, true);
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
    EntityService mockService =
        initMockServiceWithContainerParents(dashboardUrn, containerUrn1, containerUrn2);

    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), dashboardUrn, this.registry, '.', mockService, true);
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
    EntityService mockService = mock(EntityService.class);

    // Datasets
    DatasetKey datasetKey =
        new DatasetKey()
            .setName("Test.A.B")
            .setOrigin(FabricType.PROD)
            .setPlatform(Urn.createFromString("urn:li:dataPlatform:kafka"));
    Urn datasetUrn = EntityKeyUtils.convertEntityKeyToUrn(datasetKey, "dataset");
    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(datasetUrn.getEntityType()),
            eq(datasetUrn),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap()));
    BrowsePathsV2 browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), datasetUrn, this.registry, '.', mockService, true);
    BrowsePathEntryArray expectedPath = new BrowsePathEntryArray();
    BrowsePathEntry entry1 = new BrowsePathEntry().setId("Test");
    BrowsePathEntry entry2 = new BrowsePathEntry().setId("A");
    expectedPath.add(entry1);
    expectedPath.add(entry2);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Charts
    Urn chartUrn = UrnUtils.getUrn(CHART_URN);
    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(chartUrn.getEntityType()),
            eq(chartUrn),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap()));
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), chartUrn, this.registry, '/', mockService, true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId("Default");
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Dashboards
    Urn dashboardUrn = UrnUtils.getUrn(DASHBOARD_URN);
    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(dashboardUrn.getEntityType()),
            eq(dashboardUrn),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap()));
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), dashboardUrn, this.registry, '/', mockService, true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId("Default");
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Data Flows
    Urn dataFlowUrn = UrnUtils.getUrn(DATA_FLOW_URN);
    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(dataFlowUrn.getEntityType()),
            eq(dataFlowUrn),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap()));
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), dataFlowUrn, this.registry, '/', mockService, true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId("Default");
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);

    // Data Jobs
    DataJobKey dataJobKey = new DataJobKey().setFlow(dataFlowUrn).setJobId("Job/A/B");
    Urn dataJobUrn = EntityKeyUtils.convertEntityKeyToUrn(dataJobKey, "dataJob");
    browsePathsV2 =
        BrowsePathV2Utils.getDefaultBrowsePathV2(
            mock(OperationContext.class), dataJobUrn, this.registry, '/', mockService, true);
    expectedPath = new BrowsePathEntryArray();
    entry1 = new BrowsePathEntry().setId(dataFlowUrn.toString()).setUrn(dataFlowUrn);
    expectedPath.add(entry1);
    Assert.assertEquals(browsePathsV2.getPath(), expectedPath);
  }

  private EntityService initMockServiceWithContainerParents(
      Urn entityUrn, Urn containerUrn1, Urn containerUrn2) throws URISyntaxException {
    EntityService mockService = mock(EntityService.class);

    final Container container1 = new Container().setContainer(containerUrn1);
    final Map<String, EnvelopedAspect> aspectMap1 = new HashMap<>();
    aspectMap1.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container1.data())));
    final EntityResponse entityResponse1 =
        new EntityResponse().setAspects(new EnvelopedAspectMap(aspectMap1));
    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(entityUrn.getEntityType()),
            eq(entityUrn),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponse1);

    final Container container2 = new Container().setContainer(containerUrn2);
    final Map<String, EnvelopedAspect> aspectMap2 = new HashMap<>();
    aspectMap2.put(
        CONTAINER_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(container2.data())));
    final EntityResponse entityResponse2 =
        new EntityResponse().setAspects(new EnvelopedAspectMap(aspectMap2));
    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(containerUrn1.getEntityType()),
            eq(containerUrn1),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(entityResponse2);

    when(mockService.getEntityV2(
            any(OperationContext.class),
            eq(containerUrn2.getEntityType()),
            eq(containerUrn2),
            eq(Collections.singleton(CONTAINER_ASPECT_NAME))))
        .thenReturn(new EntityResponse().setAspects(new EnvelopedAspectMap()));

    return mockService;
  }
}
