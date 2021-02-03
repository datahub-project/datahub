package com.linkedin.datahub.graphql.types.mappers;

import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpUserMapper implements ModelMapper<com.linkedin.identity.CorpUser, CorpUser> {

    public static final CorpUserMapper INSTANCE = new CorpUserMapper();

    public static CorpUser map(@Nonnull final com.linkedin.identity.CorpUser corpUser) {
        return INSTANCE.apply(corpUser);
    }

    @Override
    public CorpUser apply(@Nonnull final com.linkedin.identity.CorpUser corpUser) {
        final CorpUser result = new CorpUser();
        result.setUrn(new CorpuserUrn(corpUser.getUsername()).toString());
        result.setType(EntityType.CORP_USER);
        result.setUsername(corpUser.getUsername());
        if (corpUser.hasInfo()) {
            result.setInfo(CorpUserInfoMapper.map(corpUser.getInfo()));
        }
        if (corpUser.hasEditableInfo()) {
            result.setEditableInfo(CorpUserEditableInfoMapper.map(corpUser.getEditableInfo()));
        }
        return result;
    }
}
