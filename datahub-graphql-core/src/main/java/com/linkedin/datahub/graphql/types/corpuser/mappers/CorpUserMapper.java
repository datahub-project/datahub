package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.key.CorpUserKey;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpUserMapper implements ModelMapper<EntityResponse, CorpUser> {

    public static final CorpUserMapper INSTANCE = new CorpUserMapper();

    public static CorpUser map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse);
    }

    @Override
    public CorpUser apply(@Nonnull final EntityResponse entityResponse) {
        final CorpUser result = new CorpUser();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.CORP_USER);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<CorpUser> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(CORP_USER_KEY_ASPECT_NAME, this::mapCorpUserKey);
        mappingHelper.mapToResult(CORP_USER_INFO_ASPECT_NAME, this::mapCorpUserInfo);
        mappingHelper.mapToResult(CORP_USER_EDITABLE_INFO_ASPECT_NAME, (corpUser, dataMap) ->
            corpUser.setEditableProperties(CorpUserEditableInfoMapper.map(new CorpUserEditableInfo(dataMap))));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (corpUser, dataMap) ->
            corpUser.setGlobalTags(GlobalTagsMapper.map(new GlobalTags(dataMap))));
        mappingHelper.mapToResult(CORP_USER_STATUS_ASPECT_NAME, (corpUser, dataMap) ->
            corpUser.setStatus(CorpUserStatusMapper.map(new CorpUserStatus(dataMap))));
        return mappingHelper.getResult();
    }

    private void mapCorpUserKey(@Nonnull CorpUser corpUser, @Nonnull DataMap dataMap) {
        CorpUserKey corpUserKey = new CorpUserKey(dataMap);
        corpUser.setUsername(corpUserKey.getUsername());
    }

    private void mapCorpUserInfo(@Nonnull CorpUser corpUser, @Nonnull DataMap dataMap) {
        CorpUserInfo corpUserInfo = new CorpUserInfo(dataMap);
        corpUser.setProperties(CorpUserPropertiesMapper.map(corpUserInfo));
        corpUser.setInfo(CorpUserInfoMapper.map(corpUserInfo));
    }
}
