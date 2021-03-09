package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.DownstreamOf;
import com.linkedin.metadata.relationship.Consumes;
import com.linkedin.metadata.relationship.Produces;

import java.util.List;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class RelationshipBuilderFromDataJobInputOutput extends BaseRelationshipBuilder<DataJobInputOutput> {
  public RelationshipBuilderFromDataJobInputOutput() {
    super(DataJobInputOutput.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull DataJobInputOutput inputOutput) {
    final List<DownstreamOf> downstreamEdges = inputOutput.getInputDatasets()
        .stream()
        .flatMap(upstreamDataset -> inputOutput.getOutputDatasets().stream()
        .map(downstreamDataset -> new DownstreamOf().setSource(downstreamDataset).setDestination(upstreamDataset)))
        .collect(Collectors.toList());

    final List<Consumes> inputsList = inputOutput.getInputDatasets()
        .stream()
        .map(inputDataset -> new Consumes().setSource(inputDataset).setDestination(urn))
        .collect(Collectors.toList());

    final List<Produces> outputsList = inputOutput.getOutputDatasets()
        .stream()
        .map(outputDataset -> new Produces().setSource(urn).setDestination(outputDataset))
        .collect(Collectors.toList());

    return Arrays.asList(
      new GraphBuilder.RelationshipUpdates(downstreamEdges, REMOVE_ALL_EDGES_FROM_SOURCE),
      new GraphBuilder.RelationshipUpdates(inputsList, REMOVE_ALL_EDGES_FROM_SOURCE),
      new GraphBuilder.RelationshipUpdates(outputsList, REMOVE_ALL_EDGES_FROM_SOURCE));
  }
}
