package com.linkedin.metadata.service;

import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.chart.ChartDataSourceType;
import com.linkedin.chart.ChartDataSourceTypeArray;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.ChartUrnArray;
import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.joda.time.DateTimeUtils;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LineageServiceTest {
  private static AuditStamp _auditStamp;
  private static EntityClient _mockClient;
  private LineageService _lineageService;
  private static final Authentication AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, "test"), "");
  private static final String SOURCE_FIELD_NAME = "source";
  private static final String UI_SOURCE = "UI";
  private static final String ACTOR_URN = "urn:li:corpuser:test";
  private static final String DATASET_URN_1 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test1,DEV)";
  private static final String DATASET_URN_2 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test2,DEV)";
  private static final String DATASET_URN_3 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test3,DEV)";
  private static final String DATASET_URN_4 =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,test4,DEV)";
  private static final String CHART_URN_1 = "urn:li:chart:(looker,baz1)";
  private static final String CHART_URN_2 = "urn:li:chart:(looker,baz2)";
  private static final String CHART_URN_3 = "urn:li:chart:(looker,baz3)";
  private static final String DASHBOARD_URN_1 = "urn:li:dashboard:(airflow,id1)";
  private static final String DASHBOARD_URN_2 = "urn:li:dashboard:(airflow,id2)";
  private static final String DATAJOB_URN_1 =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test,prod),test1)";
  private static final String DATAJOB_URN_2 =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test,prod),test2)";
  private static final String DATAJOB_URN_3 =
      "urn:li:dataJob:(urn:li:dataFlow:(airflow,test,prod),test3)";
  private Urn actorUrn;
  private Urn datasetUrn1;
  private Urn datasetUrn2;
  private Urn datasetUrn3;
  private Urn datasetUrn4;
  private Urn chartUrn1;
  private Urn chartUrn2;
  private Urn chartUrn3;
  private Urn dashboardUrn1;
  private Urn dashboardUrn2;
  private Urn datajobUrn1;
  private Urn datajobUrn2;
  private Urn datajobUrn3;

  @BeforeMethod
  public void setupTest() {
    DateTimeUtils.setCurrentMillisFixed(123L);
    _auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(ACTOR_URN)).setTime(123L);
    _mockClient = Mockito.mock(EntityClient.class);
    actorUrn = UrnUtils.getUrn(ACTOR_URN);
    datasetUrn1 = UrnUtils.getUrn(DATASET_URN_1);
    datasetUrn2 = UrnUtils.getUrn(DATASET_URN_2);
    datasetUrn3 = UrnUtils.getUrn(DATASET_URN_3);
    datasetUrn4 = UrnUtils.getUrn(DATASET_URN_4);
    chartUrn1 = UrnUtils.getUrn(CHART_URN_1);
    chartUrn2 = UrnUtils.getUrn(CHART_URN_2);
    chartUrn3 = UrnUtils.getUrn(CHART_URN_3);
    dashboardUrn1 = UrnUtils.getUrn(DASHBOARD_URN_1);
    dashboardUrn2 = UrnUtils.getUrn(DASHBOARD_URN_2);
    datajobUrn1 = UrnUtils.getUrn(DATAJOB_URN_1);
    datajobUrn2 = UrnUtils.getUrn(DATAJOB_URN_2);
    datajobUrn3 = UrnUtils.getUrn(DATAJOB_URN_3);

    _lineageService = new LineageService(_mockClient);
  }

  // TODO: Add tests for permissions once we add permissions for updating lineage

  // Adds upstream for dataset1 to dataset2 and removes edge to dataset3
  @Test
  public void testUpdateDatasetLineage() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn3, AUTHENTICATION)).thenReturn(true);

    UpstreamLineage upstreamLineage =
        createUpstreamLineage(new ArrayList<>(Arrays.asList(DATASET_URN_3, DATASET_URN_4)));

    Mockito.when(
            _mockClient.getV2(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(datasetUrn1),
                Mockito.eq(ImmutableSet.of(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)),
                Mockito.eq(AUTHENTICATION)))
        .thenReturn(
            new EntityResponse()
                .setUrn(datasetUrn1)
                .setEntityName(Constants.DATASET_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.UPSTREAM_LINEAGE_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(upstreamLineage.data()))))));

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.singletonList(datasetUrn3);
    _lineageService.updateDatasetLineage(
        datasetUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION);

    // upstreamLineage without dataset3, keep dataset4, add dataset2
    final UpstreamLineage updatedDataset1UpstreamLineage =
        createUpstreamLineage(new ArrayList<>(Arrays.asList(DATASET_URN_4, DATASET_URN_2)));
    final MetadataChangeProposal proposal1 = new MetadataChangeProposal();
    proposal1.setEntityUrn(UrnUtils.getUrn(DATASET_URN_1));
    proposal1.setEntityType(Constants.DATASET_ENTITY_NAME);
    proposal1.setAspectName(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    proposal1.setAspect(GenericRecordUtils.serializeAspect(updatedDataset1UpstreamLineage));
    proposal1.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal1), Mockito.eq(AUTHENTICATION), Mockito.eq(false));
  }

  @Test
  public void testFailUpdateWithMissingDataset() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(false);

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.singletonList(datasetUrn3);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            _lineageService.updateDatasetLineage(
                datasetUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  @Test
  public void testFailUpdateDatasetWithInvalidEdge() throws Exception {
    Mockito.when(_mockClient.exists(chartUrn1, AUTHENTICATION)).thenReturn(true);

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(chartUrn1);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        RuntimeException.class,
        () ->
            _lineageService.updateDatasetLineage(
                datasetUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  // Adds upstream for chart1 to dataset3 and removes edge to dataset1 while keeping edge to
  // dataset2
  @Test
  public void testUpdateChartLineage() throws Exception {
    Mockito.when(_mockClient.exists(chartUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn3, AUTHENTICATION)).thenReturn(true);

    ChartInfo chartInfo =
        createChartInfo(
            chartUrn1, Arrays.asList(datasetUrn1, datasetUrn2), Collections.emptyList());

    Mockito.when(
            _mockClient.getV2(
                Mockito.eq(Constants.CHART_ENTITY_NAME),
                Mockito.eq(chartUrn1),
                Mockito.eq(ImmutableSet.of(Constants.CHART_INFO_ASPECT_NAME)),
                Mockito.eq(AUTHENTICATION)))
        .thenReturn(
            new EntityResponse()
                .setUrn(chartUrn1)
                .setEntityName(Constants.CHART_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.CHART_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(chartInfo.data()))))));

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn3);
    final List<Urn> upstreamUrnsToRemove = Collections.singletonList(datasetUrn2);
    _lineageService.updateChartLineage(
        chartUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION);

    // chartInfo with dataset1 in inputs and dataset3 in inputEdges
    ChartInfo updatedChartInfo =
        createChartInfo(
            chartUrn1,
            Collections.singletonList(datasetUrn1),
            Collections.singletonList(datasetUrn3));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(chartUrn1);
    proposal.setEntityType(Constants.CHART_ENTITY_NAME);
    proposal.setAspectName(Constants.CHART_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(updatedChartInfo));
    proposal.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.eq(AUTHENTICATION), Mockito.eq(false));
  }

  @Test
  public void testFailUpdateChartWithMissingDataset() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(false);

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            _lineageService.updateChartLineage(
                chartUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  @Test
  public void testFailUpdateChartWithInvalidEdge() throws Exception {
    Mockito.when(_mockClient.exists(chartUrn2, AUTHENTICATION)).thenReturn(true);

    // charts can't have charts upstream of them
    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(chartUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        RuntimeException.class,
        () ->
            _lineageService.updateChartLineage(
                chartUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  // Adds upstreams for dashboard to dataset2 and chart2 and removes edge to dataset1 and chart1
  @Test
  public void testUpdateDashboardLineage() throws Exception {
    Mockito.when(_mockClient.exists(dashboardUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(chartUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(chartUrn2, AUTHENTICATION)).thenReturn(true);

    // existing dashboardInfo has upstreams to dataset1, dataset3, chart1, chart3
    DashboardInfo dashboardInfo =
        createDashboardInfo(
            dashboardUrn1,
            Arrays.asList(chartUrn1, chartUrn3),
            Collections.emptyList(),
            Arrays.asList(datasetUrn1, datasetUrn3),
            Collections.emptyList());

    Mockito.when(
            _mockClient.getV2(
                Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
                Mockito.eq(dashboardUrn1),
                Mockito.eq(ImmutableSet.of(Constants.DASHBOARD_INFO_ASPECT_NAME)),
                Mockito.eq(AUTHENTICATION)))
        .thenReturn(
            new EntityResponse()
                .setUrn(dashboardUrn1)
                .setEntityName(Constants.DASHBOARD_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DASHBOARD_INFO_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(dashboardInfo.data()))))));

    final List<Urn> upstreamUrnsToAdd = Arrays.asList(datasetUrn2, chartUrn2);
    final List<Urn> upstreamUrnsToRemove = Arrays.asList(datasetUrn1, chartUrn1);
    _lineageService.updateDashboardLineage(
        dashboardUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION);

    // dashboardInfo with chartUrn3 in charts, chartUrn2 in chartEdges, datasetUrn3 in datasets,
    // datasetUrn2 in datasetEdges
    DashboardInfo updatedDashboardInfo =
        createDashboardInfo(
            dashboardUrn1,
            Collections.singletonList(chartUrn3),
            Collections.singletonList(chartUrn2),
            Arrays.asList(datasetUrn3),
            Collections.singletonList(datasetUrn2));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(dashboardUrn1);
    proposal.setEntityType(Constants.DASHBOARD_ENTITY_NAME);
    proposal.setAspectName(Constants.DASHBOARD_INFO_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(updatedDashboardInfo));
    proposal.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.eq(AUTHENTICATION), Mockito.eq(false));
  }

  @Test
  public void testFailUpdateDashboardWithMissingDataset() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(false);

    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(datasetUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            _lineageService.updateDashboardLineage(
                dashboardUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  @Test
  public void testFailUpdateDashboardWithInvalidEdge() throws Exception {
    Mockito.when(_mockClient.exists(dashboardUrn2, AUTHENTICATION)).thenReturn(true);

    // dashboards can't have dashboards upstream of them
    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(dashboardUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        RuntimeException.class,
        () ->
            _lineageService.updateDashboardLineage(
                dashboardUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  // Adds upstream datajob3, upstream dataset3, downstream dataset4, removes upstream datajob2,
  // upstream dataset1, downstream dataset1
  // has existing upstream datajob2, upstream dataset1 and dataset2, downstream dataset4
  // Should result in upstream datajob3, upstream dataset3 and dataset2, downstream dataset5
  @Test
  public void testUpdateDataJobLineage() throws Exception {
    Mockito.when(_mockClient.exists(datajobUrn1, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datajobUrn3, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn3, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datajobUrn2, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn2, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn4, AUTHENTICATION)).thenReturn(true);
    Mockito.when(_mockClient.exists(datasetUrn1, AUTHENTICATION)).thenReturn(true);

    DataJobInputOutput firstDataJobInputOutput =
        createDataJobInputOutput(
            datajobUrn1,
            Arrays.asList(datasetUrn1, datasetUrn2),
            Collections.emptyList(),
            Collections.singletonList(datajobUrn2),
            Collections.emptyList(),
            Collections.singletonList(datasetUrn1),
            Collections.emptyList());

    DataJobInputOutput secondDataJobInputOutput =
        createDataJobInputOutput(
            datajobUrn1,
            Arrays.asList(datasetUrn1),
            Arrays.asList(datasetUrn3),
            Collections.emptyList(),
            Arrays.asList(datajobUrn3),
            Arrays.asList(datasetUrn1),
            Collections.emptyList());

    Mockito.when(
            _mockClient.getV2(
                Mockito.eq(Constants.DATA_JOB_ENTITY_NAME),
                Mockito.eq(datajobUrn1),
                Mockito.eq(ImmutableSet.of(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)),
                Mockito.eq(AUTHENTICATION)))
        .thenReturn(
            new EntityResponse()
                .setUrn(datajobUrn1)
                .setEntityName(Constants.DATA_JOB_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(firstDataJobInputOutput.data()))))),
            new EntityResponse()
                .setUrn(datajobUrn1)
                .setEntityName(Constants.DATA_JOB_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(secondDataJobInputOutput.data()))))));

    final List<Urn> upstreamUrnsToAdd = Arrays.asList(datajobUrn3, datasetUrn3);
    final List<Urn> upstreamUrnsToRemove = Arrays.asList(datajobUrn2, datasetUrn2);
    _lineageService.updateDataJobUpstreamLineage(
        datajobUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION);

    final List<Urn> downstreamUrnsToAdd = Arrays.asList(datasetUrn4);
    final List<Urn> downstreamUrnsToRemove = Arrays.asList(datasetUrn1);
    _lineageService.updateDataJobDownstreamLineage(
        datajobUrn1, downstreamUrnsToAdd, downstreamUrnsToRemove, actorUrn, AUTHENTICATION);

    DataJobInputOutput updatedDataJobInputOutput =
        createDataJobInputOutput(
            datajobUrn1,
            Arrays.asList(datasetUrn1),
            Arrays.asList(datasetUrn3),
            Collections.emptyList(),
            Arrays.asList(datajobUrn3),
            Collections.emptyList(),
            Collections.singletonList(datasetUrn4));

    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(datajobUrn1);
    proposal.setEntityType(Constants.DATA_JOB_ENTITY_NAME);
    proposal.setAspectName(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME);
    proposal.setAspect(GenericRecordUtils.serializeAspect(updatedDataJobInputOutput));
    proposal.setChangeType(ChangeType.UPSERT);
    Mockito.verify(_mockClient, Mockito.times(1))
        .ingestProposal(Mockito.eq(proposal), Mockito.eq(AUTHENTICATION), Mockito.eq(false));
  }

  @Test
  public void testFailUpdateUpstreamDataJobWithMissingUrnToAdd() throws Exception {
    Mockito.when(_mockClient.exists(datajobUrn3, AUTHENTICATION)).thenReturn(false);

    final List<Urn> upstreamUrnsToAdd = Arrays.asList(datajobUrn3);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            _lineageService.updateDataJobUpstreamLineage(
                dashboardUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  @Test
  public void testFailUpdateUpstreamDataJobWithInvalidEdge() throws Exception {
    Mockito.when(_mockClient.exists(dashboardUrn2, AUTHENTICATION)).thenReturn(true);

    // dataJobs can't have dashboards upstream of them
    final List<Urn> upstreamUrnsToAdd = Collections.singletonList(dashboardUrn2);
    final List<Urn> upstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        RuntimeException.class,
        () ->
            _lineageService.updateDataJobUpstreamLineage(
                datajobUrn1, upstreamUrnsToAdd, upstreamUrnsToRemove, actorUrn, AUTHENTICATION));
  }

  @Test
  public void testFailUpdateDownstreamDataJobWithMissingUrnToAdd() throws Exception {
    Mockito.when(_mockClient.exists(datasetUrn1, AUTHENTICATION)).thenReturn(false);

    final List<Urn> downstreamUrnsToAdd = Arrays.asList(datasetUrn1);
    final List<Urn> downstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            _lineageService.updateDataJobDownstreamLineage(
                dashboardUrn1,
                downstreamUrnsToAdd,
                downstreamUrnsToRemove,
                actorUrn,
                AUTHENTICATION));
  }

  @Test
  public void testFailUpdateDownstreamDataJobWithInvalidEdge() throws Exception {
    Mockito.when(_mockClient.exists(dashboardUrn2, AUTHENTICATION)).thenReturn(true);

    // dataJobs can't have dashboards downstream of them
    final List<Urn> downstreamUrnsToAdd = Collections.singletonList(dashboardUrn2);
    final List<Urn> downstreamUrnsToRemove = Collections.emptyList();
    assertThrows(
        RuntimeException.class,
        () ->
            _lineageService.updateDataJobUpstreamLineage(
                datajobUrn1,
                downstreamUrnsToAdd,
                downstreamUrnsToRemove,
                actorUrn,
                AUTHENTICATION));
  }

  private UpstreamLineage createUpstreamLineage(List<String> upstreamUrns) throws Exception {
    UpstreamLineage upstreamLineage = new UpstreamLineage();
    UpstreamArray upstreams = new UpstreamArray();
    for (String upstreamUrn : upstreamUrns) {
      Upstream upstream = new Upstream();
      upstream.setDataset(DatasetUrn.createFromString(upstreamUrn));
      upstream.setAuditStamp(_auditStamp);
      upstream.setCreated(_auditStamp);
      upstream.setType(DatasetLineageType.TRANSFORMED);
      final StringMap properties = new StringMap();
      properties.put(SOURCE_FIELD_NAME, UI_SOURCE);
      upstream.setProperties(properties);
      upstreams.add(upstream);
    }
    upstreamLineage.setUpstreams(upstreams);
    return upstreamLineage;
  }

  private ChartInfo createChartInfo(Urn entityUrn, List<Urn> inputsToAdd, List<Urn> inputEdgesToAdd)
      throws Exception {
    ChartInfo chartInfo = new ChartInfo();
    ChartDataSourceTypeArray inputs = new ChartDataSourceTypeArray();
    for (Urn input : inputsToAdd) {
      DatasetUrn datasetUrn = DatasetUrn.createFromUrn(input);
      inputs.add((ChartDataSourceType.create(datasetUrn)));
    }
    chartInfo.setInputs(inputs);

    EdgeArray inputEdges = new EdgeArray();
    for (Urn inputEdgeToAdd : inputEdgesToAdd) {
      addNewEdge(inputEdgeToAdd, entityUrn, inputEdges);
    }
    chartInfo.setInputEdges(inputEdges);

    return chartInfo;
  }

  private DashboardInfo createDashboardInfo(
      Urn entityUrn,
      List<Urn> chartsToAdd,
      List<Urn> chartEdgesToAdd,
      List<Urn> datasetsToAdd,
      List<Urn> datasetEdgesToAdd)
      throws Exception {
    final DashboardInfo dashboardInfo = new DashboardInfo();

    final ChartUrnArray charts = new ChartUrnArray();
    for (Urn chartUrn : chartsToAdd) {
      charts.add(ChartUrn.createFromUrn(chartUrn));
    }
    dashboardInfo.setCharts(charts);

    final EdgeArray chartEdges = new EdgeArray();
    for (Urn chartUrn : chartEdgesToAdd) {
      addNewEdge(chartUrn, entityUrn, chartEdges);
    }
    dashboardInfo.setChartEdges(chartEdges);

    final UrnArray datasets = new UrnArray();
    datasets.addAll(datasetsToAdd);
    dashboardInfo.setDatasets(datasets);

    final EdgeArray datasetEdges = new EdgeArray();
    for (Urn datasetUrn : datasetEdgesToAdd) {
      addNewEdge(datasetUrn, entityUrn, datasetEdges);
    }
    dashboardInfo.setDatasetEdges(datasetEdges);

    return dashboardInfo;
  }

  private DataJobInputOutput createDataJobInputOutput(
      Urn entityUrn,
      List<Urn> inputDatasetsToAdd,
      List<Urn> inputDatasetEdgesToAdd,
      List<Urn> inputDatajobsToAdd,
      List<Urn> inputDatajobEdgesToAdd,
      List<Urn> outputDatasetsToAdd,
      List<Urn> outputDatasetEdgesToAdd)
      throws Exception {
    final DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();

    final DatasetUrnArray inputDatasets = new DatasetUrnArray();
    for (Urn datasetUrn : inputDatasetsToAdd) {
      inputDatasets.add(DatasetUrn.createFromUrn(datasetUrn));
    }
    dataJobInputOutput.setInputDatasets(inputDatasets);

    final EdgeArray inputDatasetEdges = new EdgeArray();
    for (Urn datasetUrn : inputDatasetEdgesToAdd) {
      addNewEdge(datasetUrn, entityUrn, inputDatasetEdges);
    }
    dataJobInputOutput.setInputDatasetEdges(inputDatasetEdges);

    final DataJobUrnArray inputDatajobs = new DataJobUrnArray();
    for (Urn datajobUrn : inputDatajobsToAdd) {
      inputDatajobs.add(DataJobUrn.createFromUrn(datajobUrn));
    }
    dataJobInputOutput.setInputDatajobs(inputDatajobs);

    final EdgeArray inputDatajobEdges = new EdgeArray();
    for (Urn datajobUrn : inputDatajobEdgesToAdd) {
      addNewEdge(datajobUrn, entityUrn, inputDatajobEdges);
    }
    dataJobInputOutput.setInputDatajobEdges(inputDatajobEdges);

    final DatasetUrnArray outputDatasets = new DatasetUrnArray();
    for (Urn datasetUrn : outputDatasetsToAdd) {
      outputDatasets.add(DatasetUrn.createFromUrn(datasetUrn));
    }
    dataJobInputOutput.setOutputDatasets(outputDatasets);

    final EdgeArray outputDatasetEdges = new EdgeArray();
    for (Urn datasetUrn : outputDatasetEdgesToAdd) {
      addNewEdge(datasetUrn, entityUrn, outputDatasetEdges);
    }
    dataJobInputOutput.setOutputDatasetEdges(outputDatasetEdges);

    return dataJobInputOutput;
  }

  private void addNewEdge(
      @Nonnull final Urn upstreamUrn,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final EdgeArray edgeArray) {
    final Edge newEdge = new Edge();
    newEdge.setDestinationUrn(upstreamUrn);
    newEdge.setSourceUrn(downstreamUrn);
    newEdge.setCreated(_auditStamp);
    newEdge.setLastModified(_auditStamp);
    newEdge.setSourceUrn(downstreamUrn);
    final StringMap properties = new StringMap();
    properties.put(SOURCE_FIELD_NAME, UI_SOURCE);
    newEdge.setProperties(properties);
    edgeArray.add(newEdge);
  }
}
