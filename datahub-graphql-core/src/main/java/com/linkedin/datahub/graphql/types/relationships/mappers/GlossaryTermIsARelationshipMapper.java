package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.GlossaryTermIsARelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class GlossaryTermIsARelationshipMapper implements
        ModelMapper<com.linkedin.common.EntityRelationships, GlossaryTermIsARelationships> {

    public static final GlossaryTermIsARelationshipMapper INSTANCE = new GlossaryTermIsARelationshipMapper();

    public static GlossaryTermIsARelationships map(
            @Nonnull final com.linkedin.common.EntityRelationships relationships) {
        return INSTANCE.apply(relationships);
    }

    @Override
    public GlossaryTermIsARelationships apply(com.linkedin.common.EntityRelationships input) {
        final GlossaryTermIsARelationships result = new GlossaryTermIsARelationships();
        System.out.println(input.toString());
        result.setEntities(input.getEntities().stream().map(
                EntityRelationshipMapper::map
        ).collect(Collectors.toList()));
        return result;
    }
}
