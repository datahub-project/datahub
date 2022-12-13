package com.linkedin.metadata.service;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableSet;
import com.linkedin.chart.ChartDataSourceTypeArray;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.ChartUrnArray;
import com.linkedin.common.Edge;
import com.linkedin.common.EdgeArray;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.data.DataMap;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.mxe.MetadataChangeProposal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.linkedin.metadata.entity.AspectUtils.*;

@Slf4j
@RequiredArgsConstructor
public class LineageService {
  private final EntityClient _entityClient;

  /**
   * Validates that a given list of urns are all datasets and all exist. Throws error if either condition is false for any urn.
   */
  public void validateDatasetUrns(@Nonnull final List<Urn> urns, @Nonnull final Authentication authentication) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)) {
        throw new IllegalArgumentException(String.format("Tried to add lineage edge with non-dataset node when we expect a dataset. Upstream urn: %s", urn));
      }
      validateUrnExists(urn, authentication);
    }
  }

  /**
   * Validates that a given list of urns are all either datasets or charts and that they exist. Otherwise, throw an error.
   */
  public void validateDashboardUpstreamUrns(@Nonnull final List<Urn> urns, @Nonnull final Authentication authentication) throws Exception {
    for (final Urn urn : urns) {
      if (!urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME) && !urn.getEntityType().equals(Constants.CHART_ENTITY_NAME)) {
        throw new IllegalArgumentException(String.format("Tried to add an upstream to a dashboard that isn't a chart or dataset. Upstream urn: %s", urn));
      }
      validateUrnExists(urn, authentication);
    }
  }

  /**
   * Validates that a given urn exists using the entityService
   */
  public void validateUrnExists(@Nonnull final Urn urn, @Nonnull final Authentication authentication) throws Exception {
    if (!_entityClient.exists(urn, authentication)) {
      throw new IllegalArgumentException(String.format("Error: urn does not exist: %s", urn));
    }
  }

  /**
   * Updates dataset lineage by taking in a list of upstreams to add and to remove and updating the existing
   * upstreamLineage aspect.
   */
  public void updateDatasetLineage(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    validateDatasetUrns(upstreamUrnsToAdd, authentication);
    // TODO: add permissions check here for entity type - or have one overall permissions check above
    try {
      MetadataChangeProposal changeProposal = buildDatasetLineageProposal(
          downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor, authentication);
      _entityClient.ingestProposal(changeProposal, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update dataset lineage for urn %s", downstreamUrn), e);
    }
  }

  /**
   * Builds an MCP of UpstreamLineage for dataset entities.
   */
  @Nonnull
  public MetadataChangeProposal buildDatasetLineageProposal(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(Constants.DATASET_ENTITY_NAME, downstreamUrn, ImmutableSet.of(Constants.UPSTREAM_LINEAGE_ASPECT_NAME), authentication);

    UpstreamLineage upstreamLineage = new UpstreamLineage();
    if (entityResponse != null && entityResponse.getAspects().containsKey(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)) {
      DataMap dataMap = entityResponse.getAspects().get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data();
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
      upstreams.add(newUpstream);
    }

    upstreams.removeIf(upstream -> upstreamUrnsToRemove.contains(upstream.getDataset()));

    upstreamLineage.setUpstreams(upstreams);

    return buildMetadataChangeProposal(
        downstreamUrn, Constants.UPSTREAM_LINEAGE_ASPECT_NAME, upstreamLineage
    );
  }

  /**
   * Updates Chart lineage by building and ingesting an MCP based on inputs.
   */
  public void updateChartLineage(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    // ensure all upstream urns are dataset urns and they exist
    validateDatasetUrns(upstreamUrnsToAdd, authentication);
    // TODO: add permissions check here for entity type - or have one overall permissions check above

    try {
      MetadataChangeProposal changeProposal = buildChartLineageProposal(
          downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor, authentication);
      _entityClient.ingestProposal(changeProposal, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update chart lineage for urn %s", downstreamUrn), e);
    }
  }

  /**
   * Builds an MCP of ChartInfo for chart entities.
   */
  @Nonnull
  public MetadataChangeProposal buildChartLineageProposal(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(Constants.CHART_ENTITY_NAME, downstreamUrn, ImmutableSet.of(Constants.CHART_INFO_ASPECT_NAME), authentication);

    if (entityResponse == null || !entityResponse.getAspects().containsKey(Constants.CHART_INFO_ASPECT_NAME)) {
      throw new RuntimeException(String.format("Failed to update chart lineage for urn %s as chart info doesn't exist", downstreamUrn));
    }

    DataMap dataMap = entityResponse.getAspects().get(Constants.CHART_INFO_ASPECT_NAME).getValue().data();
    ChartInfo chartInfo = new ChartInfo(dataMap);
    if (!chartInfo.hasInputEdges()) {
      chartInfo.setInputEdges(new EdgeArray());
    }
    if (!chartInfo.hasInputs()) {
      chartInfo.setInputs(new ChartDataSourceTypeArray());
    }

    final ChartDataSourceTypeArray inputs = chartInfo.getInputs();
    final EdgeArray inputEdges = chartInfo.getInputEdges();
    final List<Urn> upstreamsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamUrnsToAdd) {
      if (
          inputEdges.stream().anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))
              || inputs.stream().anyMatch(input -> input.equals(upstreamUrn))
      ) {
        continue;
      }
      upstreamsToAdd.add(upstreamUrn);
    }

    for (final Urn upstreamUrn : upstreamsToAdd) {
      addNewEdge(upstreamUrn, downstreamUrn, actor, inputEdges);
    }

    inputEdges.removeIf(inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
    inputs.removeIf(input ->  upstreamUrnsToRemove.contains(input.getDatasetUrn()));

    chartInfo.setInputEdges(inputEdges);
    chartInfo.setInputs(inputs);

    return buildMetadataChangeProposal(downstreamUrn, Constants.CHART_INFO_ASPECT_NAME, chartInfo);
  }

  /**
   * Updates Dashboard lineage by building and ingesting an MCP based on inputs.
   */
  public void updateDashboardLineage(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    validateDashboardUpstreamUrns(upstreamUrnsToAdd, authentication);
    // TODO: add permissions check here for entity type - or have one overall permissions check above

    try {
      MetadataChangeProposal changeProposal = buildDashboardLineageProposal(
          downstreamUrn, upstreamUrnsToAdd, upstreamUrnsToRemove, actor, authentication);
      _entityClient.ingestProposal(changeProposal, authentication, false);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to update chart lineage for urn %s", downstreamUrn), e);
    }
  }

  /**
   * Builds an MCP of DashboardInfo for dashboard entities. DashboardInfo has a list of chart urns and dataset urns pointing upstream.
   * We need to filter out the chart urns and dataset urns separately in upstreamUrnsToAdd to add them to the correct fields.
   */
  @Nonnull
  public MetadataChangeProposal buildDashboardLineageProposal(
      @Nonnull final Urn downstreamUrn,
      @Nonnull final List<Urn> upstreamUrnsToAdd,
      @Nonnull final List<Urn> upstreamUrnsToRemove,
      @Nonnull final Urn actor,
      @Nonnull final Authentication authentication
  ) throws Exception {
    EntityResponse entityResponse =
        _entityClient.getV2(Constants.DASHBOARD_ENTITY_NAME, downstreamUrn, ImmutableSet.of(Constants.DASHBOARD_INFO_ASPECT_NAME), authentication);

    if (entityResponse == null || !entityResponse.getAspects().containsKey(Constants.DASHBOARD_INFO_ASPECT_NAME)) {
      throw new RuntimeException(String.format("Failed to update dashboard lineage for urn %s as dashboard info doesn't exist", downstreamUrn));
    }

    DataMap dataMap = entityResponse.getAspects().get(Constants.DASHBOARD_INFO_ASPECT_NAME).getValue().data();
    DashboardInfo dashboardInfo = new DashboardInfo(dataMap);

    // first, deal with chart edges
    updateUpstreamCharts(dashboardInfo, upstreamUrnsToAdd, upstreamUrnsToRemove, downstreamUrn, actor);

    // next, deal with dataset edges
    updateUpstreamDatasets(dashboardInfo, upstreamUrnsToAdd, upstreamUrnsToRemove, downstreamUrn, actor);

    return buildMetadataChangeProposal(downstreamUrn, Constants.DASHBOARD_INFO_ASPECT_NAME, dashboardInfo);
  }

  /**
   * Updates the charts and chartEdges fields on the DashboardInfo aspect. First, add any new lineage edges not already represented
   * in the existing fields to chartEdges. Then, remove all lineage edges from charts and chartEdges fields that are in upstreamUrnsToRemove.
   * Then update the DashboardInfo aspect.
   */
  private void updateUpstreamCharts(DashboardInfo dashboardInfo, List<Urn> upstreamUrnsToAdd, List<Urn> upstreamUrnsToRemove, Urn dashboardUrn, Urn actor) {
    initializeChartEdges(dashboardInfo);

    final List<Urn> upstreamChartUrnsToAdd =
        upstreamUrnsToAdd.stream().filter(urn -> urn.getEntityType().equals(Constants.CHART_ENTITY_NAME)).collect(Collectors.toList());
    final ChartUrnArray charts = dashboardInfo.getCharts();
    final EdgeArray chartEdges = dashboardInfo.getChartEdges();

    final List<Urn> upstreamsChartsToAdd = getUpstreamChartToAdd(upstreamChartUrnsToAdd, chartEdges, charts);

    for (final Urn upstreamUrn : upstreamsChartsToAdd) {
      addNewEdge(upstreamUrn, dashboardUrn, actor, chartEdges);
    }

    removeChartLineageEdges(chartEdges, charts, upstreamUrnsToRemove);

    dashboardInfo.setChartEdges(chartEdges);
    dashboardInfo.setCharts(charts);
  }


  private void initializeChartEdges(DashboardInfo dashboardInfo) {
    if (!dashboardInfo.hasChartEdges()) {
      dashboardInfo.setChartEdges(new EdgeArray());
    }
    if (!dashboardInfo.hasCharts()) {
      dashboardInfo.setCharts(new ChartUrnArray());
    }
  }

  /**
   * Need to filter out any existing upstream chart urns in order to get a list of net new chart urns to add to dashboard lineage
   */
  private List<Urn> getUpstreamChartToAdd(List<Urn> upstreamChartUrnsToAdd, List<Edge> chartEdges, ChartUrnArray charts) {
    final List<Urn> upstreamsChartsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamChartUrnsToAdd) {
      if (
          chartEdges.stream().anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))
              || charts.stream().anyMatch(chart -> chart.equals(upstreamUrn))
      ) {
        continue;
      }
      upstreamsChartsToAdd.add(upstreamUrn);
    }
    return upstreamsChartsToAdd;
  }

  private void removeChartLineageEdges(List<Edge> chartEdges, ChartUrnArray charts, List<Urn> upstreamUrnsToRemove) {
    chartEdges.removeIf(inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
    charts.removeIf(upstreamUrnsToRemove::contains);
  }

  /**
   * Updates the datasets and datasetEdges fields on the DashboardInfo aspect. First, add any new lineage edges not already represented
   * in the existing fields to datasetEdges.Then, remove all lineage edges from datasets and datasetEdges fields that are in upstreamUrnsToRemove.
   * Then update the DashboardInfo aspect.
   */
  private void updateUpstreamDatasets(DashboardInfo dashboardInfo, List<Urn> upstreamUrnsToAdd, List<Urn> upstreamUrnsToRemove, Urn dashboardUrn, Urn actor) {
    initializeDatasetEdges(dashboardInfo);

    final List<Urn> upstreamDatasetUrnsToAdd =
        upstreamUrnsToAdd.stream().filter(urn -> urn.getEntityType().equals(Constants.DATASET_ENTITY_NAME)).collect(Collectors.toList());
    final UrnArray datasets = dashboardInfo.getDatasets();
    final EdgeArray datasetEdges = dashboardInfo.getDatasetEdges();

    final List<Urn> upstreamsDatasetsToAdd = getUpstreamDatasetsToAdd(upstreamDatasetUrnsToAdd, datasetEdges, datasets);

    for (final Urn upstreamUrn : upstreamsDatasetsToAdd) {
      addNewEdge(upstreamUrn, dashboardUrn, actor, datasetEdges);
    }

    removeDatasetLineageEdges(datasetEdges, datasets, upstreamUrnsToRemove);

    dashboardInfo.setDatasetEdges(datasetEdges);
    dashboardInfo.setDatasets(datasets);
  }

  private void initializeDatasetEdges(DashboardInfo dashboardInfo) {
    if (!dashboardInfo.hasDatasetEdges()) {
      dashboardInfo.setDatasetEdges(new EdgeArray());
    }
    if (!dashboardInfo.hasDatasets()) {
      dashboardInfo.setDatasets(new UrnArray());
    }
  }

  private List<Urn> getUpstreamDatasetsToAdd(List<Urn> upstreamDatasetUrnsToAdd, List<Edge> datasetEdges, UrnArray datasets) {
    final List<Urn> upstreamsDatasetsToAdd = new ArrayList<>();
    for (Urn upstreamUrn : upstreamDatasetUrnsToAdd) {
      if (
          datasetEdges.stream().anyMatch(inputEdge -> inputEdge.getDestinationUrn().equals(upstreamUrn))
              || datasets.stream().anyMatch(chart -> chart.equals(upstreamUrn))
      ) {
        continue;
      }
      upstreamsDatasetsToAdd.add(upstreamUrn);
    }
    return upstreamsDatasetsToAdd;
  }

  private void removeDatasetLineageEdges(List<Edge> datasetEdges, UrnArray datasets, List<Urn> upstreamUrnsToRemove) {
    datasetEdges.removeIf(inputEdge -> upstreamUrnsToRemove.contains(inputEdge.getDestinationUrn()));
    datasets.removeIf(upstreamUrnsToRemove::contains);
  }

  private void addNewEdge(
      @Nonnull final Urn upstreamUrn,
      @Nonnull final Urn downstreamUrn,
      @Nonnull final Urn actor,
      @Nonnull final EdgeArray edgeArray
  ) {
    final Edge newEdge = new Edge();
    newEdge.setDestinationUrn(upstreamUrn);
    newEdge.setSourceUrn(downstreamUrn);
    newEdge.setCreated(getAuditStamp(actor));
    newEdge.setLastModified(getAuditStamp(actor));
    newEdge.setSourceUrn(downstreamUrn);
    edgeArray.add(newEdge);
  }
}
