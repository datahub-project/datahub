package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.Ownership;

import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.TagSnapshot;
import com.linkedin.tag.TagProperties;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class TagSnapshotMapper implements ModelMapper<TagSnapshot, Tag> {

    public static final TagSnapshotMapper INSTANCE = new TagSnapshotMapper();

    public static Tag map(@Nonnull final TagSnapshot tag) {
        return INSTANCE.apply(tag);
    }

    @Override
    public Tag apply(@Nonnull final TagSnapshot tag) {
        final Tag result = new Tag();
        result.setUrn(tag.getUrn().toString());
        result.setType(EntityType.TAG);
        result.setName(tag.getUrn().getName());

        ModelUtils.getAspectsFromSnapshot(tag).forEach(aspect -> {
            if (aspect instanceof TagProperties) {
                if (TagProperties.class.cast(aspect).hasDescription()) {
                    result.setDescription(TagProperties.class.cast(aspect).getDescription());
                }
            } else if (aspect instanceof Ownership) {
                result.setOwnership(OwnershipMapper.map((Ownership) aspect));
            }
        });
        return result;
    }
}
