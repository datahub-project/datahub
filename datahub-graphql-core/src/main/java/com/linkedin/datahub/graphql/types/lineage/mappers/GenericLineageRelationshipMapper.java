package com.linkedin.datahub.graphql.types.lineage.mappers;

import com.linkedin.datahub.graphql.generated.Chart;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.GenericLineageRelationship;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

public class GenericLineageRelationshipMapper implements ModelMapper<com.linkedin.common.lineage.GenericLineageRelationship, GenericLineageRelationship> {

    public static final GenericLineageRelationshipMapper INSTANCE = new GenericLineageRelationshipMapper();

    public static GenericLineageRelationship map(@Nonnull final com.linkedin.common.lineage.GenericLineageRelationship relationship) {
        return INSTANCE.apply(relationship);
    }

    @Override
    public GenericLineageRelationship apply(@Nonnull final com.linkedin.common.lineage.GenericLineageRelationship relationship) {
        final GenericLineageRelationship result = new GenericLineageRelationship();

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
        if (relationship.hasAuditStamp()) {
            result.setAuditStamp(AuditStampMapper.map(relationship.getAuditStamp()));
        }
        return result;
    }
}
