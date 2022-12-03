package com.linkedin.datahub.graphql.types.corpuser.mappers;

import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserAppearanceSettings;
import com.linkedin.datahub.graphql.generated.CorpUserViewsSettings;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.tag.mappers.GlobalTagsMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.identity.CorpUserCredentials;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserStatus;
import com.linkedin.metadata.key.CorpUserKey;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
public class CorpUserMapper {

    public static final CorpUserMapper INSTANCE = new CorpUserMapper();

    public static CorpUser map(@Nonnull final EntityResponse entityResponse) {
        return INSTANCE.apply(entityResponse, null);
    }

    public static CorpUser map(@Nonnull final EntityResponse entityResponse, @Nullable final FeatureFlags featureFlags) {
        return INSTANCE.apply(entityResponse, featureFlags);
    }

    public CorpUser apply(@Nonnull final EntityResponse entityResponse, @Nullable final FeatureFlags featureFlags) {
        final CorpUser result = new CorpUser();
        Urn entityUrn = entityResponse.getUrn();

        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.CORP_USER);
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<CorpUser> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(CORP_USER_KEY_ASPECT_NAME, this::mapCorpUserKey);
        mappingHelper.mapToResult(CORP_USER_INFO_ASPECT_NAME, this::mapCorpUserInfo);
        mappingHelper.mapToResult(CORP_USER_EDITABLE_INFO_ASPECT_NAME, (corpUser, dataMap) ->
            corpUser.setEditableProperties(CorpUserEditableInfoMapper.map(new CorpUserEditableInfo(dataMap))));
        mappingHelper.mapToResult(GLOBAL_TAGS_ASPECT_NAME, (corpUser, dataMap) ->
            corpUser.setGlobalTags(GlobalTagsMapper.map(new GlobalTags(dataMap), entityUrn)));
        mappingHelper.mapToResult(CORP_USER_STATUS_ASPECT_NAME,
            (corpUser, dataMap) -> corpUser.setStatus(CorpUserStatusMapper.map(new CorpUserStatus(dataMap))));
        mappingHelper.mapToResult(CORP_USER_CREDENTIALS_ASPECT_NAME, this::mapIsNativeUser);

        mapCorpUserSettings(result, aspectMap.getOrDefault(CORP_USER_SETTINGS_ASPECT_NAME, null), featureFlags);

        return mappingHelper.getResult();
    }

    private void mapCorpUserSettings(@Nonnull CorpUser corpUser, EnvelopedAspect envelopedAspect, FeatureFlags featureFlags) {
        CorpUserSettings corpUserSettings = new CorpUserSettings();
        if (envelopedAspect != null) {
            corpUserSettings = new CorpUserSettings(envelopedAspect.getValue().data());
        }
        com.linkedin.datahub.graphql.generated.CorpUserSettings result =
            new com.linkedin.datahub.graphql.generated.CorpUserSettings();

        // Map Appearance Settings -- Appearance settings always exist.
        result.setAppearance(mapCorpUserAppearanceSettings(corpUserSettings, featureFlags));

        // Map Views Settings.
        if (corpUserSettings.hasViews()) {
            result.setViews(mapCorpUserViewsSettings(corpUserSettings.getViews()));
        }

        corpUser.setSettings(result);
    }

    @Nonnull
    private CorpUserAppearanceSettings mapCorpUserAppearanceSettings(
        @Nonnull final CorpUserSettings corpUserSettings,
        @Nullable final FeatureFlags featureFlags
    ) {
        CorpUserAppearanceSettings appearanceResult = new CorpUserAppearanceSettings();
        if (featureFlags != null) {
            appearanceResult.setShowSimplifiedHomepage(featureFlags.isShowSimplifiedHomepageByDefault());
        } else {
            appearanceResult.setShowSimplifiedHomepage(false);
        }

        if (corpUserSettings.hasAppearance()) {
            appearanceResult.setShowSimplifiedHomepage(corpUserSettings.getAppearance().isShowSimplifiedHomepage());
        }
        return appearanceResult;
    }

    @Nonnull
    private CorpUserViewsSettings mapCorpUserViewsSettings(@Nonnull final com.linkedin.identity.CorpUserViewsSettings viewsSettings) {
        CorpUserViewsSettings viewsResult = new CorpUserViewsSettings();

        if (viewsSettings.hasDefaultView()) {
            final DataHubView unresolvedView = new DataHubView();
            unresolvedView.setUrn(viewsSettings.getDefaultView().toString());
            unresolvedView.setType(EntityType.DATAHUB_VIEW);
            viewsResult.setDefaultView(unresolvedView);
        }

        return viewsResult;
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

    private void mapIsNativeUser(@Nonnull CorpUser corpUser, @Nonnull DataMap dataMap) {
        CorpUserCredentials corpUserCredentials = new CorpUserCredentials(dataMap);
        boolean isNativeUser =
            corpUserCredentials != null && corpUserCredentials.hasSalt() && corpUserCredentials.hasHashedPassword();
        corpUser.setIsNativeUser(isNativeUser);
    }
}
