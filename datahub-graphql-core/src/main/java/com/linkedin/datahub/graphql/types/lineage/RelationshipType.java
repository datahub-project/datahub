package com.linkedin.datahub.graphql.types.lineage;

import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.RelationshipKey;
import com.linkedin.datahub.graphql.types.relationships.mappers.EntityRelationshipMapper;
import com.linkedin.lineage.client.RelationshipClient;
import com.linkedin.metadata.query.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class RelationshipType {

    private final RelationshipClient _relationshipClient;

    public RelationshipType(final RelationshipClient relationshipClient) {
        _relationshipClient = relationshipClient;
    }

    public List<DataFetcherResult<List<com.linkedin.datahub.graphql.generated.EntityRelationship>>> batchLoad(final List<RelationshipKey> keys, final QueryContext context) {

        try {
            return keys.stream().map(key-> {
                try {
                    EntityRelationships incomingRelationships =
                        _relationshipClient.getRelationships(key.getUrn(), RelationshipDirection.INCOMING, key.getIncomingRelationshipTypes());
                    EntityRelationships outGoingRelationships =
                        _relationshipClient.getRelationships(key.getUrn(), RelationshipDirection.INCOMING, key.getIncomingRelationshipTypes());

                    Stream<EntityRelationship> allRelationships =
                        Stream.of(incomingRelationships.getEntities(), outGoingRelationships.getEntities())
                            .flatMap(Collection::stream);

                    return DataFetcherResult.<List<com.linkedin.datahub.graphql.generated.EntityRelationship>>newResult().data(
                        allRelationships.map(
                            EntityRelationshipMapper::map
                        ).collect(Collectors.toList())
                        ).build();
                } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to batch load DownstreamLineage for entity %s", key.getUrn()), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }
}
