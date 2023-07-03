package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateCorpUserViewsSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Resolver responsible for updating the authenticated user's View-specific settings.
 */
@Slf4j
@RequiredArgsConstructor
public class UpdateCorpUserViewsSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateCorpUserViewsSettingsInput input = bindArgument(environment.getArgument("input"), UpdateCorpUserViewsSettingsInput.class);

    return CompletableFuture.supplyAsync(() -> {
      try {

        final Urn userUrn = UrnUtils.getUrn(context.getActorUrn());

        final CorpUserSettings maybeSettings = _settingsService.getCorpUserSettings(
            userUrn,
            context.getAuthentication()
        );

        final CorpUserSettings newSettings = maybeSettings == null
            ? new CorpUserSettings().setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false))
            : maybeSettings;

        // Patch the new corp user settings. This does a R-M-F.
        updateCorpUserSettings(newSettings, input);

        _settingsService.updateCorpUserSettings(
            userUrn,
            newSettings,
            context.getAuthentication()
        );
        return true;
      } catch (Exception e) {
        log.error("Failed to perform user view settings update against input {}, {}", input.toString(), e.getMessage());
        throw new RuntimeException(String.format("Failed to perform update to user view settings against input %s", input.toString()), e);
      }
    });
  }

  private static void updateCorpUserSettings(
      @Nonnull final CorpUserSettings settings,
      @Nonnull final UpdateCorpUserViewsSettingsInput input) {
    final CorpUserViewsSettings newViewSettings = settings.hasViews()
        ? settings.getViews()
        : new CorpUserViewsSettings();
    updateCorpUserViewsSettings(newViewSettings, input);
    settings.setViews(newViewSettings);
  }

  private static void updateCorpUserViewsSettings(
      @Nonnull final CorpUserViewsSettings settings,
      @Nonnull final UpdateCorpUserViewsSettingsInput input) {
    settings.setDefaultView(input.getDefaultView() != null
        ? UrnUtils.getUrn(input.getDefaultView())
        : null,
        SetMode.REMOVE_IF_NULL);
  }
}
