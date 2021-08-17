package com.linkedin.datahub.graphql.types.lineage;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlossaryTermIsARelationships;
import com.linkedin.datahub.graphql.types.relationships.mappers.GlossaryTermIsARelationshipMapper;
import com.linkedin.lineage.client.RelationshipClient;
import com.linkedin.metadata.query.RelationshipDirection;

import com.linkedin.datahub.graphql.types.LoadableType;
import graphql.execution.DataFetcherResult;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import com.linkedin.r2.RemoteInvocationException;

public class GlossaryTermsIsARelationshipsType implements LoadableType<GlossaryTermIsARelationships> {
    private final RelationshipClient _relationshipsClient;
    private final RelationshipDirection _direction =  RelationshipDirection.OUTGOING;

    public GlossaryTermsIsARelationshipsType(final RelationshipClient relationshipsClient) {
        _relationshipsClient = relationshipsClient;
    }

    @Override
    public Class<GlossaryTermIsARelationships> objectClass() {
        return GlossaryTermIsARelationships.class;
    }

    @Override
    public List<DataFetcherResult<GlossaryTermIsARelationships>> batchLoad(final List<String> keys, final QueryContext context) {

        try {
            return keys.stream().map(urn -> {
                try {
                    com.linkedin.common.EntityRelationships relationships =
                            _relationshipsClient.getRelationships(urn, _direction, "IsA");
                    return DataFetcherResult.<GlossaryTermIsARelationships>newResult().data(GlossaryTermIsARelationshipMapper.map(relationships)).build();
                } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to batch load isA term for glossary %s", urn), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load isA term for glossary", e);
        }
    }
}
