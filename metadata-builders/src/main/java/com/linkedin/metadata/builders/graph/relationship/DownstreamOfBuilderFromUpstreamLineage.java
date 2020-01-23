package com.linkedin.metadata.builders.graph.relationship;

import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.metadata.builders.graph.GraphBuilder;
import com.linkedin.metadata.relationship.DownstreamOf;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.dao.internal.BaseGraphWriterDAO.RemovalOption.*;


public class DownstreamOfBuilderFromUpstreamLineage extends BaseRelationshipBuilder<UpstreamLineage> {
  public DownstreamOfBuilderFromUpstreamLineage() {
    super(UpstreamLineage.class);
  }

  @Nonnull
  @Override
  public List<GraphBuilder.RelationshipUpdates> buildRelationships(@Nonnull Urn urn, @Nonnull UpstreamLineage upstreamLineage) {
    if (upstreamLineage.getUpstreams().isEmpty()) {
      return Collections.emptyList();
    }
    final List<DownstreamOf> downstreamEdges = upstreamLineage.getUpstreams().stream()
        .map(upstream -> new DownstreamOf()
            .setSource(urn)
            .setDestination(upstream.getDataset())
            .setType(upstream.getType())
        )
        .collect(Collectors.toList());

    return Collections.singletonList(
        new GraphBuilder.RelationshipUpdates(downstreamEdges, REMOVE_ALL_EDGES_FROM_SOURCE)
    );
  }
}
