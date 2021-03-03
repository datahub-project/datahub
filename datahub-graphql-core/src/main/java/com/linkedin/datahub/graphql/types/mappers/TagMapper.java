package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Tag;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class TagMapper implements ModelMapper<com.linkedin.tag.Tag, Tag> {

    public static final TagMapper INSTANCE = new TagMapper();

    public static Tag map(@Nonnull final com.linkedin.tag.Tag tag) {
        return INSTANCE.apply(tag);
    }

    @Override
    public Tag apply(@Nonnull final com.linkedin.tag.Tag tag) {
        final Tag result = new Tag();
        result.setUrn((new TagUrn(tag.getName()).toString()));
        result.setType(EntityType.TAG);
        result.setName(tag.getName());
        if (tag.hasDescription()) {
            result.setDescription(tag.getDescription());
        }
        if (tag.hasOwnership()) {
            result.setOwnership(OwnershipMapper.map(tag.getOwnership()));
        }
        return result;
    }
}
