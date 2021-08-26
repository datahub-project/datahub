package com.linkedin.datahub.graphql.types.lineage;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataFlowDataJobsRelationships;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.relationships.mappers.DataFlowDataJobsRelationshipsMapper;
import com.linkedin.lineage.client.Relationships;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;

import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class DataFlowDataJobsRelationshipsType implements LoadableType<DataFlowDataJobsRelationships> {

    private final Relationships _relationshipsClient;
    private final RelationshipDirection _direction = RelationshipDirection.INCOMING;

    public DataFlowDataJobsRelationshipsType(final Relationships relationshipsClient) {
        _relationshipsClient = relationshipsClient;
    }

    @Override
    public Class<DataFlowDataJobsRelationships> objectClass() {
        return DataFlowDataJobsRelationships.class;
    }

    @Override
    public List<DataFetcherResult<DataFlowDataJobsRelationships>> batchLoad(final List<String> keys, final QueryContext context) {
        try {
            return keys.stream().map(urn -> {
                try {
                    com.linkedin.common.EntityRelationships relationships =
                            _relationshipsClient.getRelationships(urn, _direction, "IsPartOf");
                    return DataFetcherResult.<DataFlowDataJobsRelationships>newResult().data(DataFlowDataJobsRelationshipsMapper.map(relationships)).build();
                } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to batch load DataJobs for DataFlow %s", urn), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load DataJobs for DataFlow", e);
        }
    }
}
