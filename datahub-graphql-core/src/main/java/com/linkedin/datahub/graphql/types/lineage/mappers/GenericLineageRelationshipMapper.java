package com.linkedin.datahub.graphql.types.lineage.mappers;

import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityRelationship;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

public class GenericLineageRelationshipMapper implements ModelMapper<com.linkedin.common.relationships.EntityRelationship, EntityRelationship> {

    public static final GenericLineageRelationshipMapper INSTANCE = new GenericLineageRelationshipMapper();

    public static EntityRelationship map(@Nonnull final com.linkedin.common.relationships.EntityRelationship relationship) {
        return INSTANCE.apply(relationship);
    }

    @Override
    public EntityRelationship apply(@Nonnull final com.linkedin.common.relationships.EntityRelationship relationship) {
        final EntityRelationship result = new EntityRelationship();

        Entity partialLineageEntity = null;
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
        if (partialLineageEntity != null) {
            result.setEntity(partialLineageEntity);
        }
        if (relationship.hasCreated()) {
            result.setCreated(AuditStampMapper.map(relationship.getCreated()));
        }
        return result;
    }
}
