package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.datahub.graphql.generated.TagUpdate;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipUpdateMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

public class TagUpdateMapper implements ModelMapper<TagUpdate, com.linkedin.tag.Tag> {

    public static final TagUpdateMapper INSTANCE = new TagUpdateMapper();

    public static com.linkedin.tag.Tag  map(@Nonnull final TagUpdate tagUpdate) {
        return INSTANCE.apply(tagUpdate);
    }

    @Override
    public com.linkedin.tag.Tag apply(@Nonnull final TagUpdate tagUpdate) {
        final com.linkedin.tag.Tag result = new com.linkedin.tag.Tag();
        result.setUrn((new TagUrn(tagUpdate.getName())));
        result.setName(tagUpdate.getName());
        if (tagUpdate.getDescription() != null) {
            result.setDescription(tagUpdate.getDescription());
        }
        if (tagUpdate.getOwnership() != null) {
            result.setOwnership(OwnershipUpdateMapper.map(tagUpdate.getOwnership()));
        }
        return result;
    }
}
