package com.linkedin.datahub.graphql.types.tag.mappers;

import com.linkedin.common.TagAssociation;
import com.linkedin.common.urn.TagUrn;
import com.linkedin.datahub.graphql.generated.TagAssociationUpdate;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

public class TagAssociationUpdateMapper implements ModelMapper<TagAssociationUpdate, TagAssociation>  {

    public static final TagAssociationUpdateMapper INSTANCE = new TagAssociationUpdateMapper();

    public static TagAssociation map(@Nonnull final TagAssociationUpdate tagAssociationUpdate) {
        return INSTANCE.apply(tagAssociationUpdate);
    }

    public TagAssociation apply(final TagAssociationUpdate tagAssociationUpdate) {
        final TagAssociation output = new TagAssociation();
        output.setTag(new TagUrn(tagAssociationUpdate.getTag().getName()));
        return output;
    }

}
