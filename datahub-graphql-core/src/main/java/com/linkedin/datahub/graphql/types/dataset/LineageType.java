package com.linkedin.datahub.graphql.types.dataset;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityRelationships;
import com.linkedin.datahub.graphql.types.LoadableType;
import com.linkedin.datahub.graphql.types.relationships.mappers.EntityRelationshipsMapper;
import com.linkedin.lineage.client.Lineages;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;

public class LineageType implements LoadableType<EntityRelationships> {

    private final Lineages _lineageClient;
    private final RelationshipDirection _direction;

    public LineageType(final Lineages lineageClient, RelationshipDirection direction) {
        _lineageClient = lineageClient;
        _direction = direction;
    }

    @Override
    public Class<EntityRelationships> objectClass() {
        return EntityRelationships.class;
    }

    @Override
    public List<EntityRelationships> batchLoad(final List<String> keys, final QueryContext context) {

        try {
            return keys.stream().map(urn -> {
                try {
                    com.linkedin.common.EntityRelationships relationships =
                            _lineageClient.getLineage(urn, _direction);
                    return EntityRelationshipsMapper.map(relationships);
                } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to batch load DownstreamLineage for dataset %s", urn), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }
}
