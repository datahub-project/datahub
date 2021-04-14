package com.linkedin.datahub.graphql.types.lineage.mappers;

import com.linkedin.datahub.graphql.generated.GenericLineage;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class GenericLineageMapper implements ModelMapper<com.linkedin.common.relationships.GenericLineage, GenericLineage> {

    public static final GenericLineageMapper INSTANCE = new GenericLineageMapper();

    public static GenericLineage map(@Nonnull final com.linkedin.common.relationships.GenericLineage lineage) {
        return INSTANCE.apply(lineage);
    }

    @Override
    public GenericLineage apply(@Nonnull final com.linkedin.common.relationships.GenericLineage input) {
        final GenericLineage result = new GenericLineage();
        result.setEntities(input.getEntities().stream().map(GenericLineageRelationshipMapper::map).collect(Collectors.toList()));
        return result;
    }
}
