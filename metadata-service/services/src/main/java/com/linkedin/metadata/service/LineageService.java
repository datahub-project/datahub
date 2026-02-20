package com.linkedin.metadata.service;

import static com.linkedin.metadata.entity.AspectUtils.*;
import static com.linkedin.metadata.utils.AuditStampUtils.getAuditStamp;

import com.google.common.collect.ImmutableSet;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.DataJobUrnArray;
import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.ml.metadata.MLModelGroupProperties;
import com.linkedin.ml.metadata.MLModelProperties;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class LineageService {
  private static final String SOURCE_FIELD_NAME = "source";
  private static final String UI_SOURCE = "UI";
  private final SystemEntityClient _entityClient;

  /**
   * Validates that a given list of urns are all datasets and all exist. Throws error if either
   * condition is false for any urn.
   */
  public void validateDatasetUrns(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> urns) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format(
                "Tried to add lineage edge with non-dataset node when we expect a dataset. Upstream urn: %s",
                urn));
      }
      validateUrnExists(opContext, urn);
    }
  }

  /**
   * Validates that a given list of urns are all either datasets or charts and that they exist.
   * Otherwise, throw an error.
   */
  public void validateDashboardUpstreamUrns(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> urns) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)
          && !urn.getEntityType().equals(Constants.CHART_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format(
                "Tried to add an upstream to a dashboard that isn't a chart or dataset. Upstream urn: %s",
                urn));
      }
      validateUrnExists(opContext, urn);
    }
  }

  /**
   * Validates that a given list of urns are all either datasets or charts and that they exist.
   * Otherwise, throw an error.
   */
  public void validateChartUpstreamUrns(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> urns) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)
          && !urn.getEntityType().equals(Constants.CHART_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format(
                "Tried to add an upstream to a chart that isn't a chart or dataset. Upstream urn: %s",
                urn));
      }
      validateUrnExists(opContext, urn);
    }
  }

  /** Validates that a given urn exists using the entityService */
  public void validateUrnExists(@Nonnull OperationContext opContext, @Nonnull final Urn urn)
      throws Exception {
    if (!_entityClient.exists(opContext, urn)) {
      throw new IllegalArgumentException(String.format("Error: urn does not exist: %s", urn));
    }
  }

  /**
   * Updates dataset lineage by taking in a list of upstreams to add and to remove and updating the
   * existing upstreamLineage aspect.
   */
  public void updateDatasetLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    validateDatasetUrns(opContext, upstreamUrnsToAdd);
    // TODO: add permissions check here for entity type - or have one overall permissions check
    // above
    try {
      MetadataChangeProposal changeProposal =
          buildDatasetLineageProposal(
              opContext, downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update dataset lineage for urn %s", downstreamUrn), e);
    }
  }

  /** Builds an MCP of UpstreamLineage for dataset entities. */
  @Nonnull
  public MetadataChangeProposal buildDatasetLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.DATASET_ENTITY_NAME,
            downstreamUrn,
            ImmutableSet.of(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            false);

    UpstreamLineage upstreamLineage = new UpstreamLineage();
    if (entityResponse != null
        && entityResponse.getAspects().containsKey(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)) {
      DataMap dataMap =
          entityResponse.getAspects().get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data();
      upstreamLineage = new UpstreamLineage(dataMap);
    }

    if (!upstreamLineage.hasUpstreams()) {
      upstreamLineage.setUpstreams(new UpstreamArray());
    }

    final UpstreamArray upstreams = upstreamLineage.getUpstreams();
    final List<Urn> upstreamsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamUrnsToAdd) {
      if (upstreams.stream().anyMatch(upstream -> upstream.getDataset().equals(upstreamUrn))) {
        continue;
      }
      upstreamsToAdd.add(upstreamUrn);
    }

    for (final Urn upstreamUrn : upstreamsToAdd) {
      final Upstream newUpstream = new Upstream();
      newUpstream.setDataset(DatasetUrn.createFromUrn(upstreamUrn));
      newUpstream.setAuditStamp(getAuditStamp(actor));
      newUpstream.setCreated(getAuditStamp(actor));
      newUpstream.setType(DatasetLineageType.TRANSFORMED);
      final StringMap properties = new StringMap();
      properties.put(SOURCE_FIELD_NAME, UI_SOURCE);
      newUpstream.setProperties(properties);
      upstreams.add(newUpstream);
    }

    upstreams.removeIf(upstream -> upstreamUrnsToRemove.contains(upstream.getDataset()));

    upstreamLineage.setUpstreams(upstreams);

    return buildMetadataChangeProposal(
        downstreamUrn, Constants.UPSTREAM_LINEAGE_ASPECT_NAME, upstreamLineage);
  }

  /** Updates Chart lineage by building and ingesting an MCP based on inputs. */
  public void updateChartLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    // ensure all upstream urns are either dataset or chart urns and they exist
    validateChartUpstreamUrns(opContext, upstreamUrnsToAdd);
    // TODO: add permissions check here for entity type - or have one overall permissions check
    // above

    try {
      MetadataChangeProposal changeProposal =
          buildChartLineageProposal(
              opContext, downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update chart lineage for urn %s", downstreamUrn), e);
    }
  }

  /** Builds an MCP of ChartInfo for chart entities. */
  @Nonnull
  public MetadataChangeProposal buildChartLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.CHART_ENTITY_NAME,
            downstreamUrn,
            ImmutableSet.of(Constants.CHART_INFO_ASPECT_NAME),
            false);

    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(Constants.CHART_INFO_ASPECT_NAME)) {
      throw new RuntimeException(
          String.format(
              "Failed to update chart lineage for urn %s as chart info doesn't exist",
              downstreamUrn));
    }

    DataMap dataMap =
        entityResponse.getAspects().get(Constants.CHART_INFO_ASPECT_NAME).getValue().data();
    ChartInfo chartInfo = new ChartInfo(dataMap);
    if (!chartInfo.hasInputEdges()) {
      chartInfo.setInputEdges(new EdgeArray());
    }

    final EdgeArray inputEdges = chartInfo.getInputEdges();
    final List<Urn> upstreamsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamUrnsToAdd) {
      // Check both inputEdges and deprecated inputs field to avoid duplicates
      if (inputEdges.stream()
          .anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))) {
        continue;
      }
      // Also check deprecated inputs field if it exists (for backwards compatibility with ingestion
      // sources)
      if (chartInfo.hasInputs()
          && chartInfo.getInputs().stream()
              .anyMatch(
                  input ->
                      input.getDatasetUrn() != null && input.getDatasetUrn().equals(upstreamUrn))) {
        continue;
      }
      upstreamsToAdd.add(upstreamUrn);
    }

    for (final Urn upstreamUrn : upstreamsToAdd) {
      addNewEdge(upstreamUrn, downstreamUrn, actor, inputEdges);
    }

    inputEdges.removeIf(inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));

    chartInfo.setInputEdges(inputEdges);

    return buildMetadataChangeProposal(downstreamUrn, Constants.CHART_INFO_ASPECT_NAME, chartInfo);
  }

  /** Updates Dashboard lineage by building and ingesting an MCP based on inputs. */
  public void updateDashboardLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    validateDashboardUpstreamUrns(opContext, upstreamUrnsToAdd);
    // TODO: add permissions check here for entity type - or have one overall permissions check
    // above

    try {
      MetadataChangeProposal changeProposal =
          buildDashboardLineageProposal(
              opContext, downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update chart lineage for urn %s", downstreamUrn), e);
    }
  }

  /**
   * Builds an MCP of DashboardInfo for dashboard entities. DashboardInfo has a list of chart urns
   * and dataset urns pointing upstream. We need to filter out the chart urns and dataset urns
   * separately in upstreamUrnsToAdd to add them to the correct fields.
   */
  @Nonnull
  public MetadataChangeProposal buildDashboardLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.DASHBOARD_ENTITY_NAME,
            downstreamUrn,
            ImmutableSet.of(Constants.DASHBOARD_INFO_ASPECT_NAME),
            false);

    if (entityResponse == null
        || !entityResponse.getAspects().containsKey(Constants.DASHBOARD_INFO_ASPECT_NAME)) {
      throw new RuntimeException(
          String.format(
              "Failed to update dashboard lineage for urn %s as dashboard info doesn't exist",
              downstreamUrn));
    }

    DataMap dataMap =
        entityResponse.getAspects().get(Constants.DASHBOARD_INFO_ASPECT_NAME).getValue().data();
    DashboardInfo dashboardInfo = new DashboardInfo(dataMap);

    // first, deal with chart edges
    updateUpstreamCharts(
        dashboardInfo, upstreamUrnsToAdd, upstreamUrnsToRemove, downstreamUrn, actor);

    // next, deal with dataset edges
    updateUpstreamDatasets(
        dashboardInfo, upstreamUrnsToAdd, upstreamUrnsToRemove, downstreamUrn, actor);

    return buildMetadataChangeProposal(
        downstreamUrn, Constants.DASHBOARD_INFO_ASPECT_NAME, dashboardInfo);
  }

  /**
   * Updates the charts and chartEdges fields on the DashboardInfo aspect. First, add any new
   * lineage edges not already represented in the existing fields to chartEdges. Then, remove all
   * lineage edges from charts and chartEdges fields that are in upstreamUrnsToRemove. Then update
   * the DashboardInfo aspect.
   */
  private void updateUpstreamCharts(
      DashboardInfo dashboardInfo,
      List<Urn> upstreamUrnsToAdd,
      List<Urn> upstreamUrnsToRemove,
      Urn dashboardUrn,
      Urn actor) {
    initializeChartEdges(dashboardInfo);

    final List<Urn> upstreamChartUrnsToAdd =
        upstreamUrnsToAdd.stream()
            .filter(urn -> urn.getEntityType().equals(Constants.CHART_ENTITY_NAME))
            .collect(Collectors.toList());
    final EdgeArray chartEdges = dashboardInfo.getChartEdges();

    final List<Urn> upstreamsChartsToAdd =
        getUpstreamChartToAdd(upstreamChartUrnsToAdd, chartEdges, dashboardInfo);

    for (final Urn upstreamUrn : upstreamsChartsToAdd) {
      addNewEdge(upstreamUrn, dashboardUrn, actor, chartEdges);
    }

    removeChartLineageEdges(chartEdges, upstreamUrnsToRemove);

    dashboardInfo.setChartEdges(chartEdges);
  }

  private void initializeChartEdges(DashboardInfo dashboardInfo) {
    if (!dashboardInfo.hasChartEdges()) {
      dashboardInfo.setChartEdges(new EdgeArray());
    }
  }

  /**
   * Need to filter out any existing upstream chart urns in order to get a list of net new chart
   * urns to add to dashboard lineage
   */
  private List<Urn> getUpstreamChartToAdd(
      List<Urn> upstreamChartUrnsToAdd, List<Edge> chartEdges, DashboardInfo dashboardInfo) {
    final List<Urn> upstreamsChartsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamChartUrnsToAdd) {
      // Check both chartEdges and deprecated charts field to avoid duplicates
      if (chartEdges.stream()
          .anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))) {
        continue;
      }
      // Also check deprecated charts field if it exists (for backwards compatibility with ingestion
      // sources)
      if (dashboardInfo.hasCharts()
          && dashboardInfo.getCharts().stream().anyMatch(chart -> chart.equals(upstreamUrn))) {
        continue;
      }
      upstreamsChartsToAdd.add(upstreamUrn);
    }
    return upstreamsChartsToAdd;
  }

  private void removeChartLineageEdges(List<Edge> chartEdges, List<Urn> upstreamUrnsToRemove) {
    chartEdges.removeIf(inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
  }

  /**
   * Updates the datasets and datasetEdges fields on the DashboardInfo aspect. First, add any new
   * lineage edges not already represented in the existing fields to datasetEdges.Then, remove all
   * lineage edges from datasets and datasetEdges fields that are in upstreamUrnsToRemove. Then
   * update the DashboardInfo aspect.
   */
  private void updateUpstreamDatasets(
      DashboardInfo dashboardInfo,
      List<Urn> upstreamUrnsToAdd,
      List<Urn> upstreamUrnsToRemove,
      Urn dashboardUrn,
      Urn actor) {
    initializeDatasetEdges(dashboardInfo);

    final List<Urn> upstreamDatasetUrnsToAdd =
        upstreamUrnsToAdd.stream()
            .filter(urn -> urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME))
            .collect(Collectors.toList());
    final EdgeArray datasetEdges = dashboardInfo.getDatasetEdges();

    final List<Urn> upstreamDatasetsToAdd =
        getUpstreamDatasetsToAdd(upstreamDatasetUrnsToAdd, datasetEdges, dashboardInfo);

    for (final Urn upstreamUrn : upstreamDatasetsToAdd) {
      addNewEdge(upstreamUrn, dashboardUrn, actor, datasetEdges);
    }

    removeDatasetLineageEdges(datasetEdges, upstreamUrnsToRemove);

    dashboardInfo.setDatasetEdges(datasetEdges);
  }

  private void initializeDatasetEdges(DashboardInfo dashboardInfo) {
    if (!dashboardInfo.hasDatasetEdges()) {
      dashboardInfo.setDatasetEdges(new EdgeArray());
    }
  }

  private List<Urn> getUpstreamDatasetsToAdd(
      List<Urn> upstreamDatasetUrnsToAdd, List<Edge> datasetEdges, DashboardInfo dashboardInfo) {
    final List<Urn> upstreamDatasetsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamDatasetUrnsToAdd) {
      // Check both datasetEdges and deprecated datasets field to avoid duplicates
      if (datasetEdges.stream()
          .anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))) {
        continue;
      }
      // Also check deprecated datasets field if it exists (for backwards compatibility with
      // ingestion sources)
      if (dashboardInfo.hasDatasets()
          && dashboardInfo.getDatasets().stream()
              .anyMatch(dataset -> dataset.equals(upstreamUrn))) {
        continue;
      }
      upstreamDatasetsToAdd.add(upstreamUrn);
    }
    return upstreamDatasetsToAdd;
  }

  private void removeDatasetLineageEdges(List<Edge> datasetEdges, List<Urn> upstreamUrnsToRemove) {
    datasetEdges.removeIf(
        inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
  }

  /**
   * Validates that a given list of urns are all either datasets or dataJobs and that they exist.
   * Otherwise, throw an error.
   */
  public void validateDataJobUpstreamUrns(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> urns) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)
          && !urn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format(
                "Tried to add an upstream to a dataJob that isn't a datJob or dataset. Upstream urn: %s",
                urn));
      }
      validateUrnExists(opContext, urn);
    }
  }

  /** Updates DataJob lineage by building and ingesting an MCP based on inputs. */
  public void updateDataJobUpstreamLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    validateDataJobUpstreamUrns(opContext, upstreamUrnsToAdd);
    // TODO: add permissions check here for entity type - or have one overall permissions check
    // above

    try {
      MetadataChangeProposal changeProposal =
          buildDataJobUpstreamLineageProposal(
              opContext, downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update chart lineage for urn %s", downstreamUrn), e);
    }
  }

  /**
   * Builds an MCP of DataJobInputOutput for datajob entities. DataJobInputOutput has a list of
   * dataset urns and datajob urns pointing upstream. We need to filter out the chart dataset and
   * datajob urns separately in upstreamUrnsToAdd to add them to the correct fields. We deal with
   * downstream pointing datasets in outputDatasets separately.
   */
  @Nonnull
  public MetadataChangeProposal buildDataJobUpstreamLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.DATA_JOB_ENTITY_NAME,
            downstreamUrn,
            ImmutableSet.of(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME),
            false);

    DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();
    if (entityResponse != null
        && entityResponse.getAspects().containsKey(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
      DataMap dataMap =
          entityResponse
              .getAspects()
              .get(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)
              .getValue()
              .data();
      dataJobInputOutput = new DataJobInputOutput(dataMap);
    }

    // first, deal with dataset edges
    updateUpstreamDatasetsForDataJobs(
        dataJobInputOutput, upstreamUrnsToAdd, upstreamUrnsToRemove, downstreamUrn, actor);

    // next, deal with dataJobs edges
    updateUpstreamDataJobs(
        dataJobInputOutput, upstreamUrnsToAdd, upstreamUrnsToRemove, downstreamUrn, actor);

    return buildMetadataChangeProposal(
        downstreamUrn, Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME, dataJobInputOutput);
  }

  /**
   * Updates the inputDatasets and inputDatasetEdges fields on the DataJobInputOutput aspect. First,
   * add any new lineage edges not already represented in the existing fields to inputDatasetEdges.
   * Then, remove all lineage edges from inputDatasets and inputDatasetEdges fields that are in
   * upstreamUrnsToRemove. Then update the DataJobInputOutput aspect.
   */
  private void updateUpstreamDatasetsForDataJobs(
      DataJobInputOutput dataJobInputOutput,
      List<Urn> upstreamUrnsToAdd,
      List<Urn> upstreamUrnsToRemove,
      Urn dashboardUrn,
      Urn actor) {
    initializeInputDatasetEdges(dataJobInputOutput);

    final List<Urn> upstreamDatasetUrnsToAdd =
        upstreamUrnsToAdd.stream()
            .filter(urn -> urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME))
            .collect(Collectors.toList());
    final EdgeArray inputDatasetEdges = dataJobInputOutput.getInputDatasetEdges();

    final List<Urn> upstreamDatasetsToAdd =
        getInputOutputDatasetsToAdd(
            upstreamDatasetUrnsToAdd,
            inputDatasetEdges,
            dataJobInputOutput.hasInputDatasets() ? dataJobInputOutput.getInputDatasets() : null);

    for (final Urn upstreamUrn : upstreamDatasetsToAdd) {
      addNewEdge(upstreamUrn, dashboardUrn, actor, inputDatasetEdges);
    }

    removeDatasetEdges(inputDatasetEdges, upstreamUrnsToRemove);

    dataJobInputOutput.setInputDatasetEdges(inputDatasetEdges);
  }

  private void initializeInputDatasetEdges(DataJobInputOutput dataJobInputOutput) {
    if (!dataJobInputOutput.hasInputDatasetEdges()) {
      dataJobInputOutput.setInputDatasetEdges(new EdgeArray());
    }
  }

  // get new dataset edges that we should be adding to inputDatasetEdges and outputDatasetEdges for
  // the DataJobInputOutput aspect
  private List<Urn> getInputOutputDatasetsToAdd(
      List<Urn> upstreamDatasetUrnsToAdd,
      List<Edge> datasetEdges,
      DatasetUrnArray deprecatedDatasets) {
    final List<Urn> upstreamDatasetsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamDatasetUrnsToAdd) {
      // Check both datasetEdges and deprecated datasets field to avoid duplicates
      if (datasetEdges.stream()
          .anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))) {
        continue;
      }
      // Also check deprecated datasets field if it exists (for backwards compatibility with
      // ingestion sources)
      if (deprecatedDatasets != null
          && deprecatedDatasets.stream().anyMatch(dataset -> dataset.equals(upstreamUrn))) {
        continue;
      }
      upstreamDatasetsToAdd.add(upstreamUrn);
    }
    return upstreamDatasetsToAdd;
  }

  private void removeDatasetEdges(List<Edge> datasetEdges, List<Urn> upstreamUrnsToRemove) {
    datasetEdges.removeIf(
        inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
  }

  /**
   * Updates the dataJobs and dataJobEdges fields on the DataJobInputOutput aspect. First, add any
   * new lineage edges not already represented in the existing fields to dataJobEdges.Then, remove
   * all lineage edges from dataJobs and dataJobEdges fields that are in upstreamUrnsToRemove. Then
   * update the DataJobInputOutput aspect.
   */
  private void updateUpstreamDataJobs(
      DataJobInputOutput dataJobInputOutput,
      List<Urn> upstreamUrnsToAdd,
      List<Urn> upstreamUrnsToRemove,
      Urn dataJobUrn,
      Urn actor) {
    initializeInputDatajobEdges(dataJobInputOutput);

    final List<Urn> upstreamDatajobUrnsToAdd =
        upstreamUrnsToAdd.stream()
            .filter(urn -> urn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME))
            .collect(Collectors.toList());
    final EdgeArray dataJobEdges = dataJobInputOutput.getInputDatajobEdges();

    final List<Urn> upstreamDatasetsToAdd =
        getInputDatajobsToAdd(
            upstreamDatajobUrnsToAdd,
            dataJobEdges,
            dataJobInputOutput.hasInputDatajobs() ? dataJobInputOutput.getInputDatajobs() : null);

    for (final Urn upstreamUrn : upstreamDatasetsToAdd) {
      addNewEdge(upstreamUrn, dataJobUrn, actor, dataJobEdges);
    }

    removeInputDatajobEdges(dataJobEdges, upstreamUrnsToRemove);

    dataJobInputOutput.setInputDatajobEdges(dataJobEdges);
  }

  private void initializeInputDatajobEdges(DataJobInputOutput dataJobInputOutput) {
    if (!dataJobInputOutput.hasInputDatajobEdges()) {
      dataJobInputOutput.setInputDatajobEdges(new EdgeArray());
    }
  }

  private List<Urn> getInputDatajobsToAdd(
      List<Urn> upstreamDatasetUrnsToAdd,
      List<Edge> dataJobEdges,
      DataJobUrnArray deprecatedDatajobs) {
    final List<Urn> upstreamDatasetsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamDatasetUrnsToAdd) {
      // Check both dataJobEdges and deprecated datajobs field to avoid duplicates
      if (dataJobEdges.stream()
          .anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))) {
        continue;
      }
      // Also check deprecated datajobs field if it exists (for backwards compatibility with
      // ingestion sources)
      if (deprecatedDatajobs != null
          && deprecatedDatajobs.stream().anyMatch(datajob -> datajob.equals(upstreamUrn))) {
        continue;
      }
      upstreamDatasetsToAdd.add(upstreamUrn);
    }
    return upstreamDatasetsToAdd;
  }

  private void removeInputDatajobEdges(List<Edge> dataJobEdges, List<Urn> upstreamUrnsToRemove) {
    dataJobEdges.removeIf(
        inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
  }

  /** Updates DataJob lineage in the downstream direction (outputDatasets and outputDatasetEdges) */
  public void updateDataJobDownstreamLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn dataJobUrn,
      @Nonnull final List<Urn> downstreamUrnsToAdd,
      @Nonnull final List<Urn> downstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    validateDatasetUrns(opContext, downstreamUrnsToAdd);
    // TODO: add permissions check here for entity type - or have one overall permissions check
    // above

    try {
      final MetadataChangeProposal changeProposal =
          buildDataJobDownstreamLineageProposal(
              opContext, dataJobUrn, downstreamUrnsToAdd, downstreamUrnsToRemove, actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update chart lineage for urn %s", dataJobUrn), e);
    }
  }

  private void initializeOutputDatajobEdges(DataJobInputOutput dataJobInputOutput) {
    if (!dataJobInputOutput.hasOutputDatasetEdges()) {
      dataJobInputOutput.setOutputDatasetEdges(new EdgeArray());
    }
  }

  /**
   * Builds an MCP of DataJobInputOutput for datajob entities. Specifically this is updating this
   * aspect for lineage in the downstream direction. This includes the fields outputDatasets
   * (deprecated) and outputDatasetEdges
   */
  @Nonnull
  public MetadataChangeProposal buildDataJobDownstreamLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn dataJobUrn,
      @Nonnull final List<Urn> downstreamUrnsToAdd,
      @Nonnull final List<Urn> downstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    final EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.DATA_JOB_ENTITY_NAME,
            dataJobUrn,
            ImmutableSet.of(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME),
            false);

    DataJobInputOutput dataJobInputOutput = new DataJobInputOutput();
    if (entityResponse != null
        && entityResponse.getAspects().containsKey(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)) {
      DataMap dataMap =
          entityResponse
              .getAspects()
              .get(Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME)
              .getValue()
              .data();
      dataJobInputOutput = new DataJobInputOutput(dataMap);
    }

    initializeOutputDatajobEdges(dataJobInputOutput);

    final EdgeArray outputDatasetEdges = dataJobInputOutput.getOutputDatasetEdges();

    final List<Urn> downstreamDatasetsToAdd =
        getInputOutputDatasetsToAdd(
            downstreamUrnsToAdd,
            outputDatasetEdges,
            dataJobInputOutput.hasOutputDatasets() ? dataJobInputOutput.getOutputDatasets() : null);

    for (final Urn downstreamUrn : downstreamDatasetsToAdd) {
      addNewEdge(downstreamUrn, dataJobUrn, actor, outputDatasetEdges);
    }

    removeDatasetEdges(outputDatasetEdges, downstreamUrnsToRemove);

    dataJobInputOutput.setOutputDatasetEdges(outputDatasetEdges);

    return buildMetadataChangeProposal(
        dataJobUrn, Constants.DATA_JOB_INPUT_OUTPUT_ASPECT_NAME, dataJobInputOutput);
  }

  public void validateMlModelLineageUrns(
      @Nonnull OperationContext opContext, @Nonnull final List<Urn> urns) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATA_JOB_ENTITY_NAME)
          && !urn.getEntityType().equals(Constants.DATA_PROCESS_INSTANCE_ENTITY_NAME)) {
        throw new IllegalArgumentException(
            String.format(
                "Tried to add lineage edge to ML Model with non-DataJob/DataProcessInstance node. Urn: %s",
                urn));
      }
      validateUrnExists(opContext, urn);
    }
  }

  public void updateMlModelLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn mlModelUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final List<Urn> downstreamUrnsToAdd,
      @Nonnull final List<Urn> downstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    validateMlModelLineageUrns(opContext, upstreamUrnsToAdd);
    validateMlModelLineageUrns(opContext, downstreamUrnsToAdd);

    try {
      MetadataChangeProposal changeProposal =
          buildMlModelLineageProposal(
              opContext,
              mlModelUrn,
              upstreamUrnsToAdd,
              upstreamUrnsToRemove,
              downstreamUrnsToAdd,
              downstreamUrnsToRemove,
              actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update ML Model lineage for urn %s", mlModelUrn), e);
    }
  }

  @Nonnull
  public MetadataChangeProposal buildMlModelLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn mlModelUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final List<Urn> downstreamUrnsToAdd,
      @Nonnull final List<Urn> downstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.ML_MODEL_ENTITY_NAME,
            mlModelUrn,
            ImmutableSet.of(Constants.ML_MODEL_PROPERTIES_ASPECT_NAME),
            false);

    MLModelProperties mlModelProperties = new MLModelProperties();
    if (entityResponse != null
        && entityResponse.getAspects().containsKey(Constants.ML_MODEL_PROPERTIES_ASPECT_NAME)) {
      DataMap dataMap =
          entityResponse
              .getAspects()
              .get(Constants.ML_MODEL_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data();
      mlModelProperties = new MLModelProperties(dataMap);
    }

    if (!mlModelProperties.hasTrainingJobs()) {
      mlModelProperties.setTrainingJobs(new UrnArray());
    }
    if (!mlModelProperties.hasDownstreamJobs()) {
      mlModelProperties.setDownstreamJobs(new UrnArray());
    }

    final UrnArray trainingJobs = mlModelProperties.getTrainingJobs();
    final UrnArray downstreamJobs = mlModelProperties.getDownstreamJobs();

    for (Urn upstreamUrn : upstreamUrnsToAdd) {
      if (!trainingJobs.contains(upstreamUrn)) {
        trainingJobs.add(upstreamUrn);
      }
    }

    for (Urn downstreamUrn : downstreamUrnsToAdd) {
      if (!downstreamJobs.contains(downstreamUrn)) {
        downstreamJobs.add(downstreamUrn);
      }
    }

    trainingJobs.removeIf(upstreamUrnsToRemove::contains);
    downstreamJobs.removeIf(downstreamUrnsToRemove::contains);

    mlModelProperties.setTrainingJobs(trainingJobs);
    mlModelProperties.setDownstreamJobs(downstreamJobs);

    return buildMetadataChangeProposal(
        mlModelUrn, Constants.ML_MODEL_PROPERTIES_ASPECT_NAME, mlModelProperties);
  }

  public void updateMlModelGroupLineage(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn mlModelGroupUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final List<Urn> downstreamUrnsToAdd,
      @Nonnull final List<Urn> downstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    validateMlModelLineageUrns(opContext, upstreamUrnsToAdd);
    validateMlModelLineageUrns(opContext, downstreamUrnsToAdd);

    try {
      MetadataChangeProposal changeProposal =
          buildMlModelGroupLineageProposal(
              opContext,
              mlModelGroupUrn,
              upstreamUrnsToAdd,
              upstreamUrnsToRemove,
              downstreamUrnsToAdd,
              downstreamUrnsToRemove,
              actor);
      _entityClient.ingestProposal(opContext, changeProposal, false);
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to update ML Model Group lineage for urn %s", mlModelGroupUrn), e);
    }
  }

  @Nonnull
  public MetadataChangeProposal buildMlModelGroupLineageProposal(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn mlModelGroupUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final List<Urn> downstreamUrnsToAdd,
      @Nonnull final List<Urn> downstreamUrnsToRemove,
      @Nonnull final Urn actor)
      throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(
            opContext,
            Constants.ML_MODEL_GROUP_ENTITY_NAME,
            mlModelGroupUrn,
            ImmutableSet.of(Constants.ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME),
            false);

    MLModelGroupProperties mlModelGroupProperties = new MLModelGroupProperties();
    if (entityResponse != null
        && entityResponse
            .getAspects()
            .containsKey(Constants.ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME)) {
      DataMap dataMap =
          entityResponse
              .getAspects()
              .get(Constants.ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME)
              .getValue()
              .data();
      mlModelGroupProperties = new MLModelGroupProperties(dataMap);
    }

    if (!mlModelGroupProperties.hasTrainingJobs()) {
      mlModelGroupProperties.setTrainingJobs(new UrnArray());
    }
    if (!mlModelGroupProperties.hasDownstreamJobs()) {
      mlModelGroupProperties.setDownstreamJobs(new UrnArray());
    }

    final UrnArray trainingJobs = mlModelGroupProperties.getTrainingJobs();
    final UrnArray downstreamJobs = mlModelGroupProperties.getDownstreamJobs();

    for (Urn upstreamUrn : upstreamUrnsToAdd) {
      if (!trainingJobs.contains(upstreamUrn)) {
        trainingJobs.add(upstreamUrn);
      }
    }

    for (Urn downstreamUrn : downstreamUrnsToAdd) {
      if (!downstreamJobs.contains(downstreamUrn)) {
        downstreamJobs.add(downstreamUrn);
      }
    }

    trainingJobs.removeIf(upstreamUrnsToRemove::contains);
    downstreamJobs.removeIf(downstreamUrnsToRemove::contains);

    mlModelGroupProperties.setTrainingJobs(trainingJobs);
    mlModelGroupProperties.setDownstreamJobs(downstreamJobs);

    return buildMetadataChangeProposal(
        mlModelGroupUrn, Constants.ML_MODEL_GROUP_PROPERTIES_ASPECT_NAME, mlModelGroupProperties);
  }

  private void addNewEdge(
      @Nonnull final Urn upstreamUrn,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final Urn actor,
      @Nonnull final EdgeArray edgeArray) {
    final Edge newEdge = new Edge();
    newEdge.setDestinationUrn(upstreamUrn);
    newEdge.setSourceUrn(downstreamUrn);
    newEdge.setCreated(getAuditStamp(actor));
    newEdge.setLastModified(getAuditStamp(actor));
    newEdge.setSourceUrn(downstreamUrn);
    final StringMap properties = new StringMap();
    properties.put(SOURCE_FIELD_NAME, UI_SOURCE);
    newEdge.setProperties(properties);
    edgeArray.add(newEdge);
  }
}
