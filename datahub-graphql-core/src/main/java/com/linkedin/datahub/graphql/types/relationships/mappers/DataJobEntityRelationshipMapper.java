package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.generated.DataJobEntityRelationship;

import javax.annotation.Nonnull;
import java.util.Objects;

public class DataJobEntityRelationshipMapper implements ModelMapper<com.linkedin.common.EntityRelationship, DataJobEntityRelationship> {

    public static final DataJobEntityRelationshipMapper INSTANCE = new DataJobEntityRelationshipMapper();

    public static DataJobEntityRelationship map(@Nonnull final com.linkedin.common.EntityRelationship relationship) {
        return INSTANCE.apply(relationship);
    }

    @Override
    public DataJobEntityRelationship apply(@Nonnull final com.linkedin.common.EntityRelationship relationship) {
        final DataJobEntityRelationship result = new DataJobEntityRelationship();

        DataJob dataJobEntity = new DataJob();
        dataJobEntity.setUrn(relationship.getEntity().toString());
        dataJobEntity.setType(EntityType.DATA_JOB);
        dataJobEntity.setJobId(relationship.getEntity().getId());
        result.setEntity(dataJobEntity);
        if (relationship.hasCreated()) {
            result.setCreated(AuditStampMapper.map(Objects.requireNonNull(relationship.getCreated())));
        }
        return result;
    }
}
