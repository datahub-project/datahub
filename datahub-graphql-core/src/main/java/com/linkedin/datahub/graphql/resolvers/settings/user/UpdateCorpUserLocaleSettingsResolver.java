package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserLocaleSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserLocaleSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for updating the authenticated user's locale settings. */
@Slf4j
@RequiredArgsConstructor
public class UpdateCorpUserLocaleSettingsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateCorpUserLocaleSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateCorpUserLocaleSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());

            final CorpUserSettings maybeSettings =
                _settingsService.getCorpUserSettings(context.getOperationContext(), userUrn);

            final CorpUserSettings newSettings =
                maybeSettings == null
                    ? new CorpUserSettings()
                        .setAppearance(
                            new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
                    : maybeSettings;

            updateLocaleSettings(newSettings, input);

            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), userUrn, newSettings);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to update user locale settings against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to update user locale settings against input %s", input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static void updateLocaleSettings(
      @Nonnull final CorpUserSettings settings,
      @Nonnull final UpdateCorpUserLocaleSettingsInput input) {
    final CorpUserLocaleSettings localeSettings =
        settings.hasLocale() ? settings.getLocale() : new CorpUserLocaleSettings();
    localeSettings.setLanguage(input.getLanguage(), SetMode.REMOVE_IF_NULL);
    settings.setLocale(localeSettings);
  }
}
