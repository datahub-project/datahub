package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.datahub.graphql.generated.BrowseResultGroup;
import com.linkedin.datahub.graphql.generated.BrowseResultMetadata;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class BrowseResultMetadataMapper implements ModelMapper<com.linkedin.metadata.query.BrowseResultMetadata, BrowseResultMetadata> {

    public static final BrowseResultMetadataMapper INSTANCE = new BrowseResultMetadataMapper();

    public static BrowseResultMetadata map(@Nonnull final com.linkedin.metadata.query.BrowseResultMetadata input) {
        return INSTANCE.apply(input);
    }

    @Override
    public BrowseResultMetadata apply(@Nonnull final com.linkedin.metadata.query.BrowseResultMetadata input) {
        final BrowseResultMetadata result = new BrowseResultMetadata();
        result.setPath(BrowsePathMapper.map(input.getPath()).getPath());
        result.setTotalNumEntities(input.getTotalNumEntities());
        result.setGroups(input.getGroups().stream().map(this::mapGroup).collect(Collectors.toList()));
        return result;
    }

    private BrowseResultGroup mapGroup(@Nonnull final com.linkedin.metadata.query.BrowseResultGroup group) {
        final BrowseResultGroup result = new BrowseResultGroup();
        result.setName(group.getName());
        result.setCount(group.getCount());
        return result;
    }
}
