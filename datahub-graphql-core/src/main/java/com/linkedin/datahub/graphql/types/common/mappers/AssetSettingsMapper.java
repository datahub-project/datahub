package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.AssetSettings;
import com.linkedin.datahub.graphql.generated.AssetSummarySettings;
import com.linkedin.datahub.graphql.generated.AssetSummarySettingsTemplate;
import com.linkedin.datahub.graphql.generated.DataHubPageTemplate;
import com.linkedin.datahub.graphql.generated.EntityType;
import java.util.List;
import javax.annotation.Nonnull;

public class AssetSettingsMapper {

  public static final AssetSettingsMapper INSTANCE = new AssetSettingsMapper();

  public static AssetSettings map(
      @Nonnull final com.linkedin.settings.asset.AssetSettings gmsSettings) {
    return INSTANCE.apply(gmsSettings);
  }

  public AssetSettings apply(@Nonnull final com.linkedin.settings.asset.AssetSettings gmsSettings) {
    AssetSettings assetSettings = new AssetSettings();

    if (gmsSettings.getAssetSummary() != null) {
      assetSettings.setAssetSummary(mapAssetSummarySettings(gmsSettings.getAssetSummary()));
    }

    return assetSettings;
  }

  private AssetSummarySettings mapAssetSummarySettings(
      @Nonnull final com.linkedin.settings.asset.AssetSummarySettings gmsSummarySettings) {
    AssetSummarySettings assetSummarySettings = new AssetSummarySettings();

    if (gmsSummarySettings.getTemplates() != null) {
      List<AssetSummarySettingsTemplate> templates =
          gmsSummarySettings.getTemplates().stream()
              .map(
                  settingsTemplate -> {
                    AssetSummarySettingsTemplate summarySettingsTemplate =
                        new AssetSummarySettingsTemplate();
                    DataHubPageTemplate pageTemplate = new DataHubPageTemplate();
                    pageTemplate.setUrn(settingsTemplate.getTemplate().toString());
                    pageTemplate.setType(EntityType.DATAHUB_PAGE_TEMPLATE);
                    summarySettingsTemplate.setTemplate(pageTemplate);
                    return summarySettingsTemplate;
                  })
              .toList();
      assetSummarySettings.setTemplates(templates);
    }

    return assetSummarySettings;
  }
}
