package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.common.GlobalTags;

import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.dao.utils.ModelUtils;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
import javax.annotation.Nonnull;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpUserSnapshotMapper implements ModelMapper<CorpUserSnapshot, CorpUser> {

    public static final CorpUserSnapshotMapper INSTANCE = new CorpUserSnapshotMapper();

    public static CorpUser map(@Nonnull final CorpUserSnapshot corpUser) {
        return INSTANCE.apply(corpUser);
    }

    @Override
    public CorpUser apply(@Nonnull final CorpUserSnapshot corpUser) {
        final CorpUser result = new CorpUser();
        result.setUrn(corpUser.getUrn().toString());
        result.setType(EntityType.CORP_USER);
        result.setUsername(corpUser.getUrn().getUsernameEntity());

        ModelUtils.getAspectsFromSnapshot(corpUser).forEach(aspect -> {
            if (aspect instanceof CorpUserInfo) {
                result.setProperties(CorpUserPropertiesMapper.map(CorpUserInfo.class.cast(aspect)));
                result.setInfo(CorpUserInfoMapper.map(CorpUserInfo.class.cast(aspect)));
            } else if (aspect instanceof CorpUserEditableInfo) {
                result.setEditableInfo(CorpUserEditableInfoMapper.map(CorpUserEditableInfo.class.cast(aspect)));
            } else if (aspect instanceof GlobalTags) {
                result.setGlobalTags(GlobalTagsMapper.map(GlobalTags.class.cast(aspect)));
            } else if (aspect instanceof CorpUserStatus) {
                result.setStatus(CorpUserStatusMapper.map(CorpUserStatus.class.cast(aspect)));
            }
        });

        return result;
    }
}
