package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.datahub.graphql.generated.CorpUserEditableProperties;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;

import javax.annotation.Nonnull;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpUserEditableInfoMapper implements ModelMapper<com.linkedin.identity.CorpUserEditableInfo, CorpUserEditableProperties> {

    public static final CorpUserEditableInfoMapper INSTANCE = new CorpUserEditableInfoMapper();

    public static CorpUserEditableProperties map(@Nonnull final com.linkedin.identity.CorpUserEditableInfo info) {
        return INSTANCE.apply(info);
    }

    @Override
    public CorpUserEditableProperties apply(@Nonnull final com.linkedin.identity.CorpUserEditableInfo info) {
        final CorpUserEditableProperties result = new CorpUserEditableProperties();
        result.setDisplayName(info.getDisplayName());
        result.setTitle(info.getTitle());
        result.setAboutMe(info.getAboutMe());
        result.setSkills(info.getSkills());
        result.setTeams(info.getTeams());
        result.setEmail(info.getEmail());
        result.setPhone(info.getPhone());
        result.setSlack(info.getSlack());
        if (info.hasPictureLink()) {
            result.setPictureLink(info.getPictureLink().toString());
        }
        return result;
    }
}
