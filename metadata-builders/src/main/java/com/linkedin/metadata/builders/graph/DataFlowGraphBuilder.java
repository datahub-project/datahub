package com.linkedin.metadata.builders.graph;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.OwnedByBuilderFromOwnership;

import com.linkedin.metadata.entity.DataFlowEntity;
import com.linkedin.metadata.snapshot.DataFlowSnapshot;


public class DataFlowGraphBuilder extends BaseGraphBuilder<DataFlowSnapshot>  {
    private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
        Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
            {
                add(new OwnedByBuilderFromOwnership());
            }
        });

    public DataFlowGraphBuilder() {
        super(DataFlowSnapshot.class, RELATIONSHIP_BUILDERS);
    }

    @Nonnull
    @Override
    protected List<? extends RecordTemplate> buildEntities(@Nonnull DataFlowSnapshot snapshot) {
        final DataFlowUrn urn = snapshot.getUrn();
        final DataFlowEntity entity = new DataFlowEntity().setUrn(urn)
            .setOrchestrator(urn.getOrchestratorEntity())
            .setFlowId(urn.getFlowIdEntity())
            .setCluster(urn.getClusterEntity());

        setRemovedProperty(snapshot, entity);

        return Collections.singletonList(entity);
    }
}
