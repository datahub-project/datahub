package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.DashboardUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.ContainsBuilderFromDashboardInfo;
import com.linkedin.metadata.entity.DashboardEntity;
import com.linkedin.metadata.snapshot.DashboardSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public class DashboardGraphBuilder extends BaseGraphBuilder<DashboardSnapshot> {

  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new ContainsBuilderFromDashboardInfo());
        }
      });

  public DashboardGraphBuilder() {
    super(DashboardSnapshot.class, RELATIONSHIP_BUILDERS);
  }

  @Nonnull
  @Override
  protected List<? extends RecordTemplate> buildEntities(@Nonnull DashboardSnapshot snapshot) {
    final DashboardUrn urn = snapshot.getUrn();
    final DashboardEntity entity = new DashboardEntity().setUrn(urn)
        .setDashboardTool(urn.getDashboardToolEntity())
        .setDashboardId(urn.getDashboardIdEntity());

    return Collections.singletonList(entity);
  }
}
