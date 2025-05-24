package com.linkedin.datahub.graphql.types.ingestion;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.IngestionSource;
import com.linkedin.datahub.graphql.resolvers.ingest.IngestionResolverUtils;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class IngestionSourceMapper implements ModelMapper<EntityResponse, IngestionSource> {
    public static final IngestionSourceMapper INSTANCE = new IngestionSourceMapper();

    public static IngestionSource map(
            @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(context, entityResponse);
    }

    @Override
    public IngestionSource apply(@Nullable QueryContext context, @Nonnull EntityResponse input) {
        return IngestionResolverUtils.mapIngestionSource(input);
    }
}
