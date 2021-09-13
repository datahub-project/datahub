package com.linkedin.metadata.builders.graph;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.linkedin.common.urn.MLModelUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.EvaluatedOnBuilderFromEvaluationData;
import com.linkedin.metadata.builders.graph.relationship.OwnedByBuilderFromOwnership;
import com.linkedin.metadata.builders.graph.relationship.TrainedOnBuilderFromTrainingData;
import com.linkedin.metadata.entity.MLModelEntity;
import com.linkedin.metadata.snapshot.MLModelSnapshot;

public class MLModelGraphBuilder extends BaseGraphBuilder<MLModelSnapshot>  {
    private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS =
        Collections.unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
            {
                add(new OwnedByBuilderFromOwnership());
                add(new TrainedOnBuilderFromTrainingData());
                add(new EvaluatedOnBuilderFromEvaluationData());
            }
        });

    public MLModelGraphBuilder() {
        super(MLModelSnapshot.class, RELATIONSHIP_BUILDERS);
    }

    @Nonnull
    @Override
    protected List<? extends RecordTemplate> buildEntities(@Nonnull MLModelSnapshot snapshot) {
        final MLModelUrn urn = snapshot.getUrn();
        final MLModelEntity entity = new MLModelEntity().setUrn(urn)
            .setName(urn.getMlModelNameEntity())
            .setPlatform(urn.getPlatformEntity())
            .setOrigin(urn.getOriginEntity());

        setRemovedProperty(snapshot, entity);

        return Collections.singletonList(entity);
    }
}
