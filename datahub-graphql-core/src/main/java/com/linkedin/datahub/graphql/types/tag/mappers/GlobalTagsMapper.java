package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.TagAssociation;
import com.linkedin.datahub.graphql.generated.Tag;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;
import java.util.stream.Collectors;

public class GlobalTagsMapper implements ModelMapper<GlobalTags, com.linkedin.datahub.graphql.generated.GlobalTags> {
    public static final GlobalTagsMapper INSTANCE = new GlobalTagsMapper();

    public static com.linkedin.datahub.graphql.generated.GlobalTags map(@Nonnull final GlobalTags standardTags) {
        return INSTANCE.apply(standardTags);
    }

    @Override
    public com.linkedin.datahub.graphql.generated.GlobalTags apply(@Nonnull final GlobalTags input) {
        final com.linkedin.datahub.graphql.generated.GlobalTags result = new com.linkedin.datahub.graphql.generated.GlobalTags();
        result.setTags(input.getTags().stream().map(this::mapTagAssociation).collect(Collectors.toList()));
        return result;
    }

    private com.linkedin.datahub.graphql.generated.TagAssociation mapTagAssociation(@Nonnull final TagAssociation input) {
        final com.linkedin.datahub.graphql.generated.TagAssociation result = new com.linkedin.datahub.graphql.generated.TagAssociation();
        final Tag resultTag = new Tag();
        resultTag.setUrn(input.getTag().toString());
        result.setTag(resultTag);
        return result;
    }
}
