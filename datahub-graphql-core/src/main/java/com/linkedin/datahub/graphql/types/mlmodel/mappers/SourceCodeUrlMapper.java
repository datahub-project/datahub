package com.linkedin.datahub.graphql.types.mlmodel.mappers;

import javax.annotation.Nonnull;

import com.linkedin.datahub.graphql.generated.SourceCodeUrl;
import com.linkedin.datahub.graphql.generated.SourceCodeUrlType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

public class SourceCodeUrlMapper implements ModelMapper<com.linkedin.ml.metadata.SourceCodeUrl, SourceCodeUrl> {
    public static final SourceCodeUrlMapper INSTANCE = new SourceCodeUrlMapper();

    public static SourceCodeUrl map(@Nonnull final com.linkedin.ml.metadata.SourceCodeUrl input) {
        return INSTANCE.apply(input);
    }

    @Override
    public SourceCodeUrl apply(@Nonnull final com.linkedin.ml.metadata.SourceCodeUrl input) {
        final SourceCodeUrl results = new SourceCodeUrl();
        results.setType(SourceCodeUrlType.valueOf(input.getType().toString()));
        results.setSourceCodeUrl(input.getSourceCodeUrl().toString());
        return results;
    }
}
