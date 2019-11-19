package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.dataset.DatasetLineageType;
import com.linkedin.dataset.Upstream;
import com.linkedin.dataset.UpstreamArray;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.relationship.DownstreamOf;
import java.util.Arrays;
import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.utils.TestUtils.*;
import static org.testng.Assert.*;


public class DownstreamOfBuilderFromUpstreamLineageTest {
  @Test
  public void testBuildRelationships() {
    DatasetUrn datasetSource = makeDatasetUrn("source");
    DatasetUrn datasetDest1 = makeDatasetUrn("dest1");
    DatasetUrn datasetDest2 = makeDatasetUrn("dest2");

    Upstream upstream1 = new Upstream()
        .setDataset(datasetDest1)
        .setType(DatasetLineageType.TRANSFORMED);
    Upstream upstream2 = new Upstream()
        .setDataset(datasetDest2)
        .setType(DatasetLineageType.COPY);
    UpstreamLineage upstreamLineage = new UpstreamLineage().setUpstreams(new UpstreamArray(Arrays.asList(upstream1, upstream2)));

    List<GraphBuilder.RelationshipUpdates> operations =
        new DownstreamOfBuilderFromUpstreamLineage().buildRelationships(datasetSource, upstreamLineage);

    assertEquals(operations.size(), 1);
    assertEquals(operations.get(0).getRelationships(),
        Arrays.asList(makeDownstreamOf(datasetSource, datasetDest1, DatasetLineageType.TRANSFORMED),
            makeDownstreamOf(datasetSource, datasetDest2, DatasetLineageType.COPY)));
    assertEquals(operations.get(0).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);
  }

  private DownstreamOf makeDownstreamOf(DatasetUrn source, DatasetUrn destination,
      DatasetLineageType type) {
    return new DownstreamOf()
        .setSource(source)
        .setDestination(destination)
        .setType(type);
  }
}
