package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.datahub.graphql.generated.DownstreamLineage;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class DownstreamLineageMapper implements ModelMapper<com.linkedin.dataset.DownstreamLineage, DownstreamLineage> {

    public static final DownstreamLineageMapper INSTANCE = new DownstreamLineageMapper();

    public static DownstreamLineage map(@Nonnull final com.linkedin.dataset.DownstreamLineage lineage) {
        return INSTANCE.apply(lineage);
    }

    @Override
    public DownstreamLineage apply(@Nonnull final com.linkedin.dataset.DownstreamLineage input) {
        final DownstreamLineage result = new DownstreamLineage();
        result.setDownstreams(input.getDownstreams().stream().map(DownstreamMapper::map).collect(Collectors.toList()));
        return result;
    }
}
