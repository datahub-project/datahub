package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.EntityRelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class EntityRelationshipsMapper implements ModelMapper<com.linkedin.common.EntityRelationships, EntityRelationships> {

    public static final EntityRelationshipsMapper INSTANCE = new EntityRelationshipsMapper();

    public static EntityRelationships map(@Nonnull final com.linkedin.common.EntityRelationships relationships) {
        return INSTANCE.apply(relationships);
    }

    @Override
    public EntityRelationships apply(@Nonnull final com.linkedin.common.EntityRelationships input) {
        final EntityRelationships result = new EntityRelationships();
        result.setEntities(input.getEntities().stream().map(
                EntityRelationshipMapper::map
        ).collect(Collectors.toList()));
        return result;
    }
}
