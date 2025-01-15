package com.linkedin.datahub.graphql.types.corpuser.mappers;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.Forms;
import com.linkedin.common.GlobalTags;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.CorpUserAppearanceSettings;
import com.linkedin.datahub.graphql.generated.CorpUserProperties;
import com.linkedin.datahub.graphql.generated.CorpUserViewsSettings;
import com.linkedin.datahub.graphql.generated.DataHubView;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.types.common.mappers.CustomPropertiesMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.form.FormsMapper;
import com.linkedin.datahub.graphql.types.structuredproperty.StructuredPropertiesMapper;
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
import com.linkedin.structured.StructuredProperties;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Maps Pegasus {@link RecordTemplate} objects to objects conforming to the GQL schema.
 *
 * <p>To be replaced by auto-generated mappers implementations
 */
public class CorpUserMapper {

  public static final CorpUserMapper INSTANCE = new CorpUserMapper();

  public static CorpUser map(
      @Nullable QueryContext context, @Nonnull final EntityResponse entityResponse) {
    return INSTANCE.apply(context, entityResponse, null);
  }

  public static CorpUser map(
      @Nullable QueryContext context,
      @Nonnull final EntityResponse entityResponse,
      @Nullable final FeatureFlags featureFlags) {
    return INSTANCE.apply(context, entityResponse, featureFlags);
  }

  public CorpUser apply(
      @Nullable QueryContext context,
      @Nonnull final EntityResponse entityResponse,
      @Nullable final FeatureFlags featureFlags) {
    final CorpUser result = new CorpUser();
    Urn entityUrn = entityResponse.getUrn();

    result.setUrn(entityResponse.getUrn().toString());
    result.setType(EntityType.CORP_USER);
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    MappingHelper<CorpUser> mappingHelper = new MappingHelper<>(aspectMap, result);
    mappingHelper.mapToResult(CORP_USER_KEY_ASPECT_NAME, this::mapCorpUserKey);
    mappingHelper.mapToResult(
        CORP_USER_INFO_ASPECT_NAME,
        (corpUser, dataMap) -> this.mapCorpUserInfo(context, corpUser, dataMap, entityUrn));
    mappingHelper.mapToResult(
        CORP_USER_EDITABLE_INFO_ASPECT_NAME,
        (corpUser, dataMap) ->
            corpUser.setEditableProperties(
                CorpUserEditableInfoMapper.map(context, new CorpUserEditableInfo(dataMap))));
    mappingHelper.mapToResult(
        GLOBAL_TAGS_ASPECT_NAME,
        (corpUser, dataMap) ->
            corpUser.setGlobalTags(
                GlobalTagsMapper.map(context, new GlobalTags(dataMap), entityUrn)));
    mappingHelper.mapToResult(
        CORP_USER_STATUS_ASPECT_NAME,
        (corpUser, dataMap) ->
            corpUser.setStatus(CorpUserStatusMapper.map(context, new CorpUserStatus(dataMap))));
    mappingHelper.mapToResult(CORP_USER_CREDENTIALS_ASPECT_NAME, this::mapIsNativeUser);
    mappingHelper.mapToResult(
        STRUCTURED_PROPERTIES_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setStructuredProperties(
                StructuredPropertiesMapper.map(
                    context, new StructuredProperties(dataMap), entityUrn))));
    mappingHelper.mapToResult(
        FORMS_ASPECT_NAME,
        ((entity, dataMap) ->
            entity.setForms(FormsMapper.map(new Forms(dataMap), entityUrn.toString()))));

    mapCorpUserSettings(
        result, aspectMap.getOrDefault(CORP_USER_SETTINGS_ASPECT_NAME, null), featureFlags);

    return mappingHelper.getResult();
  }

  private void mapCorpUserSettings(
      @Nonnull CorpUser corpUser, EnvelopedAspect envelopedAspect, FeatureFlags featureFlags) {
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
      @Nonnull final CorpUserSettings corpUserSettings, @Nullable final FeatureFlags featureFlags) {
    CorpUserAppearanceSettings appearanceResult = new CorpUserAppearanceSettings();
    if (featureFlags != null) {
      appearanceResult.setShowSimplifiedHomepage(featureFlags.isShowSimplifiedHomepageByDefault());
    } else {
      appearanceResult.setShowSimplifiedHomepage(false);
    }

    if (corpUserSettings.hasAppearance()) {
      appearanceResult.setShowSimplifiedHomepage(
          corpUserSettings.getAppearance().isShowSimplifiedHomepage());
    }
    return appearanceResult;
  }

  @Nonnull
  private CorpUserViewsSettings mapCorpUserViewsSettings(
      @Nonnull final com.linkedin.identity.CorpUserViewsSettings viewsSettings) {
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

  private void mapCorpUserInfo(
      @Nullable QueryContext context,
      @Nonnull CorpUser corpUser,
      @Nonnull DataMap dataMap,
      @Nonnull Urn entityUrn) {
    CorpUserInfo corpUserInfo = new CorpUserInfo(dataMap);
    corpUser.setProperties(CorpUserPropertiesMapper.map(context, corpUserInfo));
    corpUser.setInfo(CorpUserInfoMapper.map(context, corpUserInfo));
    CorpUserProperties corpUserProperties = corpUser.getProperties();
    if (corpUserInfo.hasCustomProperties()) {
      corpUserProperties.setCustomProperties(
          CustomPropertiesMapper.map(corpUserInfo.getCustomProperties(), entityUrn));
    }
    corpUser.setProperties(corpUserProperties);
  }

  private void mapIsNativeUser(@Nonnull CorpUser corpUser, @Nonnull DataMap dataMap) {
    CorpUserCredentials corpUserCredentials = new CorpUserCredentials(dataMap);
    boolean isNativeUser =
        corpUserCredentials != null
            && corpUserCredentials.hasSalt()
            && corpUserCredentials.hasHashedPassword();
    corpUser.setIsNativeUser(isNativeUser);
  }
}
