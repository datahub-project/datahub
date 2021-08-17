package com.linkedin.datahub.graphql.types.relationships.mappers;

import com.linkedin.datahub.graphql.generated.GlossaryTermHasARelationships;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class GlossaryTermHasARelationshipMapper implements
        ModelMapper<com.linkedin.common.EntityRelationships, GlossaryTermHasARelationships> {

    public static final GlossaryTermHasARelationshipMapper INSTANCE = new GlossaryTermHasARelationshipMapper();

    public static GlossaryTermHasARelationships map(
            @Nonnull final com.linkedin.common.EntityRelationships relationships) {
        return INSTANCE.apply(relationships);
    }

    @Override
    public GlossaryTermHasARelationships apply(com.linkedin.common.EntityRelationships input) {
        final GlossaryTermHasARelationships result = new GlossaryTermHasARelationships();
        result.setEntities(input.getEntities().stream().map(
                EntityRelationshipMapper::map
        ).collect(Collectors.toList()));
        return result;
    }
}
