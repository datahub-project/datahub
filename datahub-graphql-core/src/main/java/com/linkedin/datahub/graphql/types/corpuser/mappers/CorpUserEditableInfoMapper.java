package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.generated.CorpUserEditableInfo;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpUserEditableInfoMapper implements ModelMapper<com.linkedin.identity.CorpUserEditableInfo, CorpUserEditableInfo> {

    public static final CorpUserEditableInfoMapper INSTANCE = new CorpUserEditableInfoMapper();

    public static CorpUserEditableInfo map(@Nonnull final com.linkedin.identity.CorpUserEditableInfo info) {
        return INSTANCE.apply(info);
    }

    @Override
    public CorpUserEditableInfo apply(@Nonnull final com.linkedin.identity.CorpUserEditableInfo info) {
        final CorpUserEditableInfo result = new CorpUserEditableInfo();
        result.setAboutMe(info.getAboutMe());
        result.setSkills(info.getSkills());
        result.setTeams(info.getTeams());
        if (info.hasPictureLink()) {
            result.setPictureLink(info.getPictureLink().toString());
        }
        return result;
    }
}
