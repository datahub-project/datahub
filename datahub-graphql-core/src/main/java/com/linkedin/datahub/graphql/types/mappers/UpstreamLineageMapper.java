package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.UpstreamLineage;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class UpstreamLineageMapper implements ModelMapper<com.linkedin.dataset.UpstreamLineage, UpstreamLineage> {

    public static final UpstreamLineageMapper INSTANCE = new UpstreamLineageMapper();

    public static UpstreamLineage map(@Nonnull final com.linkedin.dataset.UpstreamLineage lineage) {
        return INSTANCE.apply(lineage);
    }

    @Override
    public UpstreamLineage apply(@Nonnull final com.linkedin.dataset.UpstreamLineage input) {
        final UpstreamLineage result = new UpstreamLineage();
        result.setUpstreams(input.getUpstreams().stream().map(UpstreamMapper::map).collect(Collectors.toList()));
        return result;
    }
}
