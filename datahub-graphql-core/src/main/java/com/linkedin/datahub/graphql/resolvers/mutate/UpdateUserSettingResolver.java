package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.UpdateUserSettingInput;
import com.linkedin.datahub.graphql.generated.UserSetting;
import com.linkedin.datahub.graphql.resolvers.settings.user.UpdateCorpUserViewsSettingsResolver;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Deprecated! Use {@link UpdateCorpUserViewsSettingsResolver} instead. */
@Slf4j
@RequiredArgsConstructor
public class UpdateUserSettingResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityService _entityService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateUserSettingInput input =
        bindArgument(environment.getArgument("input"), UpdateUserSettingInput.class);

    UserSetting name = input.getName();
    final boolean value = input.getValue();
    final Urn actor = UrnUtils.getUrn(context.getActorUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            // In the future with more settings, we'll need to do a read-modify-write
            // for now though, we can just write since there is only 1 setting
            CorpUserSettings newSettings =
                getCorpUserSettings(context.getOperationContext(), actor);
            CorpUserAppearanceSettings appearanceSettings =
                newSettings.hasAppearance()
                    ? newSettings.getAppearance()
                    : new CorpUserAppearanceSettings();
            if (name.equals(UserSetting.SHOW_SIMPLIFIED_HOMEPAGE)) {
              newSettings.setAppearance(appearanceSettings.setShowSimplifiedHomepage(value));
            } else if (name.equals(UserSetting.SHOW_THEME_V2)) {
              newSettings.setAppearance(appearanceSettings.setShowThemeV2(value));
            } else {
              log.error("User Setting name {} not currently supported", name);
              throw new RuntimeException(
                  String.format("User Setting name %s not currently supported", name));
            }

            MetadataChangeProposal proposal =
                buildMetadataChangeProposalWithUrn(
                    actor, CORP_USER_SETTINGS_ASPECT_NAME, newSettings);

            _entityService.ingestProposal(
                context.getOperationContext(), proposal, EntityUtils.getAuditStamp(actor), false);

            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform user settings update against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to perform user settings update against input %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  @Nonnull
  private CorpUserSettings getCorpUserSettings(
      @Nonnull OperationContext opContext, @Nonnull final Urn urn) {
    CorpUserSettings settings =
        (CorpUserSettings)
            _entityService.getAspect(opContext, urn, CORP_USER_SETTINGS_ASPECT_NAME, 0);
    return settings == null ? new CorpUserSettings() : settings;
  }
}
