package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpGroupMapper implements ModelMapper<com.linkedin.identity.CorpGroup, CorpGroup> {

    public static final CorpGroupMapper INSTANCE = new CorpGroupMapper();

    public static CorpGroup map(@Nonnull final com.linkedin.identity.CorpGroup corpGroup) {
        return INSTANCE.apply(corpGroup);
    }

    @Override
    public CorpGroup apply(@Nonnull final com.linkedin.identity.CorpGroup corpGroup) {
        final CorpGroup result = new CorpGroup();
        result.setUrn(new CorpGroupUrn(corpGroup.getName()).toString());
        result.setType(EntityType.CORP_GROUP);
        result.setName(corpGroup.getName());
        if (corpGroup.hasInfo()) {
            result.setInfo(CorpGroupInfoMapper.map(corpGroup.getInfo()));
        }
        return result;
    }
}
