package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.chart.ChartDataSourceType;
import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.DownstreamOf;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class DownstreamOfBuilderFromChartInfo extends BaseRelationshipBuilder<ChartInfo> {
  public DownstreamOfBuilderFromChartInfo() {
    super(ChartInfo.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull ChartInfo chartInfo) {
    if (chartInfo.getInputs() == null || chartInfo.getInputs().isEmpty()) {
      return Collections.emptyList();
    }
    final List<DownstreamOf> downstreamEdges = chartInfo.getInputs().stream()
        .map(upstream -> new DownstreamOf()
            .setSource(urn)
            .setDestination(getUpstreamUrn(upstream))
        )
        .collect(Collectors.toList());

    return Collections.singletonList(
        new GraphBuilder.RelationshipUpdates(downstreamEdges, REMOVE_ALL_EDGES_FROM_SOURCE)
    );
  }

  @Nonnull
  private Urn getUpstreamUrn(@Nonnull ChartDataSourceType upstream) {
    if (upstream.isDatasetUrn()) {
      return upstream.getDatasetUrn();
    }
    throw new IllegalArgumentException("Unknown chart data source type!");
  }
}
