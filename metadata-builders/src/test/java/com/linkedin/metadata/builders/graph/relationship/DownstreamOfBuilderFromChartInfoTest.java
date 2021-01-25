package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.chart.ChartInfo;
import com.linkedin.common.urn.ChartUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.relationship.DownstreamOf;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.builders.common.ChartTestUtils.*;
import static com.linkedin.metadata.testing.Urns.*;
import static org.testng.Assert.*;


public class DownstreamOfBuilderFromChartInfoTest {
  @Test
  public void testBuildRelationships() {
    ChartUrn chartSource = makeChartUrn("source");
    ChartInfo chartInfo = makeChartInfo();

    List<GraphBuilder.RelationshipUpdates> operations =
        new DownstreamOfBuilderFromChartInfo().buildRelationships(chartSource, chartInfo);

    assertEquals(operations.size(), 1);
    assertEquals(operations.get(0).getRelationships(),
        Arrays.asList(makeDownstreamOf(chartSource, makeDatasetUrn("fooDataset"))));
    assertEquals(operations.get(0).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);
  }

  private DownstreamOf makeDownstreamOf(ChartUrn source, DatasetUrn destination) {
    return new DownstreamOf()
        .setSource(source)
        .setDestination(destination);
  }
}
