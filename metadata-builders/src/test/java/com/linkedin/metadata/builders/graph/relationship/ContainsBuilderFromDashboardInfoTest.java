package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.dashboard.DashboardInfo;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.relationship.Contains;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.builders.common.DashboardTestUtils.*;
import static com.linkedin.metadata.testing.Urns.*;
import static org.testng.Assert.*;


public class ContainsBuilderFromDashboardInfoTest {
  @Test
  public void testBuildRelationships() {
    DashboardUrn dashboardSource = makeDashboardUrn("source");
    DashboardInfo dashboardInfo = makeDashboardInfo();

    List<GraphBuilder.RelationshipUpdates> operations =
        new ContainsBuilderFromDashboardInfo().buildRelationships(dashboardSource, dashboardInfo);

    assertEquals(operations.size(), 1);
    assertEquals(operations.get(0).getRelationships(),
        Arrays.asList(makeContains(dashboardSource, makeChartUrn("1"))));
    assertEquals(operations.get(0).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);
  }

  private Contains makeContains(DashboardUrn source, ChartUrn destination) {
    return new Contains()
        .setSource(source)
        .setDestination(destination);
  }
}
