package com.linkedin.datahub.graphql.types.lineage.mappers;

import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetLineageType;
import com.linkedin.datahub.graphql.generated.GenericLineageRelationship;
import com.linkedin.datahub.graphql.generated.LineageEntity;
import com.linkedin.datahub.graphql.generated.RelatedDataset;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.dataset.Downstream;

import javax.annotation.Nonnull;

public class GenericLineageRelationshipMapper implements ModelMapper<com.linkedin.common.lineage.GenericLineageRelationship, GenericLineageRelationship> {

    public static final GenericLineageRelationshipMapper INSTANCE = new GenericLineageRelationshipMapper();

    public static GenericLineageRelationship map(@Nonnull final com.linkedin.common.lineage.GenericLineageRelationship relationship) {
        return INSTANCE.apply(relationship);
    }

    @Override
    public GenericLineageRelationship apply(@Nonnull final com.linkedin.common.lineage.GenericLineageRelationship relationship) {
        final GenericLineageRelationship result = new GenericLineageRelationship();

        LineageEntity lineageEntity;
        if (relationship.getEntity().getEntityType() === 'dataset') {
           lineageEntity = new Dataset();
           lineageEntity.
        }
        final Dataset partialDataset = new Dataset();
        lineageEntity.setUrn(relationship.getDataset().toString());
        result.setEntity(partialDataset);
        result.setAuditStamp(AuditStampMapper.map(relationship.getAuditStamp()));
        return result;
    }
}
