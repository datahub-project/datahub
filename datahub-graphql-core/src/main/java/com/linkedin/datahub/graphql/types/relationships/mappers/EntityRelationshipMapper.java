package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DataJob;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.generated.EntityWithRelationships;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
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

        EntityWithRelationships partialLineageEntity = null;
        if (relationship.getEntity().getEntityType().equals("dataset")) {
           partialLineageEntity = new Dataset();
            ((Dataset) partialLineageEntity).setUrn(relationship.getEntity().toString());
        }
        if (relationship.getEntity().getEntityType().equals("chart")) {
            partialLineageEntity = new Chart();
            ((Chart) partialLineageEntity).setUrn(relationship.getEntity().toString());
        }
        if (relationship.getEntity().getEntityType().equals("dashboard")) {
            partialLineageEntity = new Dashboard();
            ((Dashboard) partialLineageEntity).setUrn(relationship.getEntity().toString());
        }
        if (relationship.getEntity().getEntityType().equals("dataJob")) {
            partialLineageEntity = new DataJob();
            ((DataJob) partialLineageEntity).setUrn(relationship.getEntity().toString());
        }
        if (partialLineageEntity != null) {
            result.setEntity(partialLineageEntity);
        }
        if (relationship.hasCreated()) {
            result.setCreated(AuditStampMapper.map(relationship.getCreated()));
        }
        return result;
    }
}
