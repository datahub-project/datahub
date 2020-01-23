package com.linkedin.metadata.builders.graph;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.ReportsToBuilderFromCorpUserInfo;
import com.linkedin.metadata.entity.CorpUserEntity;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;


public class CorpUserGraphBuilder extends BaseGraphBuilder<CorpUserSnapshot> {

  private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
      Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
        {
          add(new ReportsToBuilderFromCorpUserInfo());
        }
      });

  public CorpUserGraphBuilder() {
    super(CorpUserSnapshot.class, RELATIONSHIP_BUILDERS);
  }

  @Nonnull
  @Override
  protected List<? extends RecordTemplate> buildEntities(@Nonnull CorpUserSnapshot snapshot) {
    final CorpuserUrn urn = snapshot.getUrn();
    final CorpUserEntity entity = new CorpUserEntity().setUrn(urn)
        .setRemoved(false)
        .setName(urn.getUsernameEntity());

    return Collections.singletonList(entity);
  }
}
