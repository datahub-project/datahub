package com.linkedin.datahub.graphql.types.lineage;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GlossaryTermHasARelationships;
import com.linkedin.datahub.graphql.types.relationships.mappers.GlossaryTermHasARelationshipMapper;
import com.linkedin.lineage.client.Relationships;
import com.linkedin.metadata.query.RelationshipDirection;

import com.linkedin.datahub.graphql.types.LoadableType;

import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import com.linkedin.r2.RemoteInvocationException;

public class GlossaryTermsHasARelationshipsType implements LoadableType<GlossaryTermHasARelationships> {
    private final Relationships _relationshipsClient;
    private final RelationshipDirection _direction =  RelationshipDirection.OUTGOING;

    public GlossaryTermsHasARelationshipsType(final Relationships relationshipsClient) {
        _relationshipsClient = relationshipsClient;
    }

    @Override
    public Class<GlossaryTermHasARelationships> objectClass() {
        return GlossaryTermHasARelationships.class;
    }

    @Override
    public List<GlossaryTermHasARelationships> batchLoad(final List<String> keys, final QueryContext context) {

        try {
            return keys.stream().map(urn -> {
                try {
                    com.linkedin.common.EntityRelationships relationships =
                            _relationshipsClient.getRelationships(urn, _direction, "HasA");
                    System.out.println("relationships is " + relationships.toString());
                    return GlossaryTermHasARelationshipMapper.map(relationships);
                } catch (RemoteInvocationException | URISyntaxException e) {
                    throw new RuntimeException(String.format("Failed to batch load isA term for glossary %s", urn), e);
                }
            }).collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load isA term for glossary", e);
        }
    }
}
