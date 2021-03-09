package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.relationship.DownstreamOf;
import com.linkedin.metadata.relationship.Consumes;
import com.linkedin.metadata.relationship.Produces;

import java.util.List;
import org.testng.annotations.Test;

import static com.linkedin.metadata.builders.common.DataJobTestUtils.*;
import static com.linkedin.metadata.testing.Urns.*;
import static org.testng.Assert.*;


public class RelationshipBuilderFromDataJobInputOutputTest {
  @Test
  public void testBuildRelationships() {
    DataJobUrn job = makeDataJobUrn("my_job");
    DataJobInputOutput inputOutput = makeDataJobInputOutput();

    List<GraphBuilder.RelationshipUpdates> operations =
        new RelationshipBuilderFromDataJobInputOutput().buildRelationships(job, inputOutput);

    assertEquals(operations.size(), 3);

    assertEquals(operations.get(0).getRelationships().size(), 4);
    assertEquals(
      operations.get(0).getRelationships().get(0),
      makeDownstreamOf(
        makeDatasetUrn("output1"),
        makeDatasetUrn("input1")));
    assertEquals(
      operations.get(0).getRelationships().get(1),
      makeDownstreamOf(
        makeDatasetUrn("output2"),
        makeDatasetUrn("input1")));
    assertEquals(
      operations.get(0).getRelationships().get(2),
      makeDownstreamOf(
        makeDatasetUrn("output1"),
        makeDatasetUrn("input2")));
    assertEquals(
      operations.get(0).getRelationships().get(3),
      makeDownstreamOf(
        makeDatasetUrn("output2"),
        makeDatasetUrn("input2")));
    assertEquals(operations.get(0).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);

    assertEquals(operations.get(1).getRelationships().size(), 2);
    assertEquals(
      operations.get(1).getRelationships().get(0),
      makeConsumes(makeDatasetUrn("input1"), job));
    assertEquals(
      operations.get(1).getRelationships().get(1),
      makeConsumes(makeDatasetUrn("input2"), job));
    assertEquals(operations.get(1).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);

    assertEquals(operations.get(2).getRelationships().size(), 2);
    assertEquals(
      operations.get(2).getRelationships().get(0),
      makeProduces(job, makeDatasetUrn("output1")));
    assertEquals(
      operations.get(2).getRelationships().get(1),
      makeProduces(job, makeDatasetUrn("output2")));
    assertEquals(operations.get(2).getPreUpdateOperation(),
        BaseGraphWriterDAO.RemovalOption.REMOVE_ALL_EDGES_FROM_SOURCE);

  }

  private DownstreamOf makeDownstreamOf(DatasetUrn source, DatasetUrn destination) {
    return new DownstreamOf()
        .setSource(source)
        .setDestination(destination);
  }

  private Consumes makeConsumes(DatasetUrn source, DataJobUrn destination) {
    return new Consumes()
        .setSource(source)
        .setDestination(destination);
  }

  private Produces makeProduces(DataJobUrn source, DatasetUrn destination) {
    return new Produces()
        .setSource(source)
        .setDestination(destination);
  }

}

