package com.linkedin.datahub.graphql.types.corpgroup.mappers;

import com.linkedin.datahub.graphql.generated.CorpGroup;
import com.linkedin.datahub.graphql.generated.CorpGroupInfo;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpGroupInfoMapper implements ModelMapper<com.linkedin.identity.CorpGroupInfo, CorpGroupInfo> {

    public static final CorpGroupInfoMapper INSTANCE = new CorpGroupInfoMapper();

    public static CorpGroupInfo map(@Nonnull final com.linkedin.identity.CorpGroupInfo corpGroupInfo) {
        return INSTANCE.apply(corpGroupInfo);
    }

    @Override
    public CorpGroupInfo apply(@Nonnull final com.linkedin.identity.CorpGroupInfo info) {
        final CorpGroupInfo result = new CorpGroupInfo();
        result.setEmail(info.getEmail());
        result.setAdmins(info.getAdmins());
        result.setMembers(info.getMembers());
        result.setGroups(info.getGroups());
        return result;
    }
}
