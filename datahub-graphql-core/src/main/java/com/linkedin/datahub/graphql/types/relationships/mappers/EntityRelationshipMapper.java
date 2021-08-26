package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.generated.EntityWithRelationships;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

public class EntityRelationshipMapper implements ModelMapper<com.linkedin.common.EntityRelationship, EntityRelationship> {

    public static final EntityRelationshipMapper INSTANCE = new EntityRelationshipMapper();

    public static EntityRelationship map(@Nonnull final com.linkedin.common.EntityRelationship relationship) {
        return INSTANCE.apply(relationship);
    }

    @Override
    public EntityRelationship apply(@Nonnull final com.linkedin.common.EntityRelationship relationship) {
        final EntityRelationship result = new EntityRelationship();

        EntityWithRelationships partialLineageEntity = (EntityWithRelationships) UrnToEntityMapper.map(relationship.getEntity());
        if (partialLineageEntity != null) {
            result.setEntity(partialLineageEntity);
        }
        if (relationship.hasCreated()) {
            result.setCreated(AuditStampMapper.map(relationship.getCreated()));
        }
        return result;
    }
}
