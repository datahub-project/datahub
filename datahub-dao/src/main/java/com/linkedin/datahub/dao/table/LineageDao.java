package com.linkedin.datahub.dao.table;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.datahub.models.view.LineageView;
import com.linkedin.dataset.Dataset;
import com.linkedin.dataset.DownstreamArray;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.client.Datasets;
import com.linkedin.dataset.client.Lineages;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.datahub.util.DatasetUtil.*;


public class LineageDao {

  private final Lineages _lineages;
  private final Datasets _datasets;

  public LineageDao(@Nonnull Lineages lineages, @Nonnull Datasets datasets) {
    _lineages = lineages;
    _datasets = datasets;
  }

  /**
   * Gets the upstream datasets of a certain dataset with lineage metadata attached
   * @param datasetUrn String
   * @return List of LineageView
   */
  public List<LineageView> getUpstreamLineage(@Nonnull String datasetUrn) throws Exception {
    final UpstreamArray upstreamArray = _lineages.getUpstreamLineage(toDatasetUrn(datasetUrn)).getUpstreams();
    final Map<DatasetUrn, Dataset> datasets = _datasets.batchGet(upstreamArray.stream().map(u -> u.getDataset())
        .collect(Collectors.toSet()));

    return upstreamArray.stream()
        .map(us -> toLineageView(datasets.get(us.getDataset()), us.getType().name(), us.getAuditStamp()))
        .collect(Collectors.toList());
  }

  /**
   * Gets the downstream datasets of a certain dataset with lineage metadata attached
   * @param datasetUrn String
   * @return List of LineageView
   */
  public List<LineageView> getDownstreamLineage(@Nonnull String datasetUrn) throws Exception {
    final DownstreamArray downstreamArray = _lineages.getDownstreamLineage(toDatasetUrn(datasetUrn)).getDownstreams();
    final Map<DatasetUrn, Dataset> datasets = _datasets.batchGet(downstreamArray.stream().map(u -> u.getDataset())
        .collect(Collectors.toSet()));

    return downstreamArray.stream()
        .map(ds -> toLineageView(datasets.get(ds.getDataset()), ds.getType().name(), ds.getAuditStamp()))
        .collect(Collectors.toList());
  }
}
