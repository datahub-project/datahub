package com.linkedin.datahub.graphql.resolvers.settings.asset;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.AssetSettings;
import com.linkedin.datahub.graphql.generated.UpdateAssetSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateAssetSummaryInput;
import com.linkedin.datahub.graphql.types.common.mappers.AssetSettingsMapper;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.settings.asset.AssetSummarySettings;
import com.linkedin.settings.asset.AssetSummarySettingsTemplate;
import com.linkedin.settings.asset.AssetSummarySettingsTemplateArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

public class UpdateAssetSettingsResolver implements DataFetcher<CompletableFuture<AssetSettings>> {

  private final EntityClient _entityClient;

  public UpdateAssetSettingsResolver(@Nonnull final EntityClient entityClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
  }

  @Override
  public CompletableFuture<AssetSettings> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateAssetSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateAssetSettingsInput.class);
    final Urn assetUrn = UrnUtils.getUrn(input.getUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // TODO: check permissions
          try {
            Aspect aspect =
                _entityClient.getLatestAspectObject(
                    context.getOperationContext(),
                    assetUrn,
                    Constants.ASSET_SETTINGS_ASPECT_NAME,
                    false);
            com.linkedin.settings.asset.AssetSettings assetSettings =
                new com.linkedin.settings.asset.AssetSettings();
            if (aspect != null) {
              assetSettings = new com.linkedin.settings.asset.AssetSettings(aspect.data());
            }

            if (input.getSummary() != null) {
              updateAssetSummary(assetSettings, input.getSummary());
            }

            _entityClient.ingestProposal(
                context.getOperationContext(),
                AspectUtils.buildMetadataChangeProposal(
                    assetUrn, Constants.ASSET_SETTINGS_ASPECT_NAME, assetSettings),
                false);

            return AssetSettingsMapper.map(assetSettings);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update action settings! %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static void updateAssetSummary(
      @Nonnull final com.linkedin.settings.asset.AssetSettings assetSettings,
      @Nonnull final UpdateAssetSummaryInput input) {
    if (input.getTemplate() != null) {
      AssetSummarySettings assetSummarySettings = new AssetSummarySettings();
      AssetSummarySettingsTemplateArray settingsTemplates = new AssetSummarySettingsTemplateArray();
      AssetSummarySettingsTemplate settingsTemplate = new AssetSummarySettingsTemplate();
      settingsTemplate.setTemplate(UrnUtils.getUrn(input.getTemplate()));
      settingsTemplates.add(settingsTemplate);
      assetSummarySettings.setTemplates(settingsTemplates);
      assetSettings.setAssetSummary(assetSummarySettings);
    }
  }
}
