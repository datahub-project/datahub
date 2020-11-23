package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.ChartUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.DownstreamOfBuilderFromChartInfo;
import com.linkedin.metadata.entity.ChartEntity;
import com.linkedin.metadata.snapshot.ChartSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public class ChartGraphBuilder extends BaseGraphBuilder<ChartSnapshot> {

  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new DownstreamOfBuilderFromChartInfo());
        }
      });

  public ChartGraphBuilder() {
    super(ChartSnapshot.class, RELATIONSHIP_BUILDERS);
  }

  @Nonnull
  @Override
  protected List<? extends RecordTemplate> buildEntities(@Nonnull ChartSnapshot snapshot) {
    final ChartUrn urn = snapshot.getUrn();
    final ChartEntity entity = new ChartEntity().setUrn(urn)
        .setDashboardTool(urn.getDashboardToolEntity())
        .setChartId(urn.getChartIdEntity());

    return Collections.singletonList(entity);
  }
}
