package com.linkedin.metadata.builders.graph;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.builders.graph.relationship.BaseRelationshipBuilder;
import com.linkedin.metadata.builders.graph.relationship.OwnedByBuilderFromOwnership;

import com.linkedin.metadata.entity.TagEntity;
import com.linkedin.metadata.snapshot.TagSnapshot;

public class TagGraphBuilder extends BaseGraphBuilder<TagSnapshot> {
    private static final Set<BaseRelationshipBuilder> RELATIONSHIP_BUILDERS = Collections
            .unmodifiableSet(new HashSet<BaseRelationshipBuilder>() {
                {
                    add(new OwnedByBuilderFromOwnership());
                }
            });

    public TagGraphBuilder() {
        super(TagSnapshot.class, RELATIONSHIP_BUILDERS);
    }

    @Nonnull
    @Override
    protected List<? extends RecordTemplate> buildEntities(@Nonnull TagSnapshot snapshot) {
        final TagUrn urn = snapshot.getUrn();
        final TagEntity entity = new TagEntity().setUrn(urn).setName(urn.getName());

        return Collections.singletonList(entity);
    }
}
