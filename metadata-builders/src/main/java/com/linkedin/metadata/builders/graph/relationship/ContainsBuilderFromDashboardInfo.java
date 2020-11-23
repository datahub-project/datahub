package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.Contains;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class ContainsBuilderFromDashboardInfo extends BaseRelationshipBuilder<DashboardInfo> {
  public ContainsBuilderFromDashboardInfo() {
    super(DashboardInfo.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull DashboardInfo dashboardInfo) {
    if (dashboardInfo.getCharts().isEmpty()) {
      return Collections.emptyList();
    }
    final List<Contains> containsEdges = dashboardInfo.getCharts().stream()
        .map(chart -> new Contains()
            .setSource(urn)
            .setDestination(chart)
        )
        .collect(Collectors.toList());

    return Collections.singletonList(
        new GraphBuilder.RelationshipUpdates(containsEdges, REMOVE_ALL_EDGES_FROM_SOURCE)
    );
  }
}
