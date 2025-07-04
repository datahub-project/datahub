package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.UpdateUserHomePageSettingsInput;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserHomePageSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for updating the authenticated user's HomePage-specific settings. */
@Slf4j
@RequiredArgsConstructor
public class UpdateUserHomePageSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateUserHomePageSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateUserHomePageSettingsInput.class);

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

            // Patch the new corp user settings. This does a R-M-F.
            updateCorpUserSettings(newSettings, input);

            _settingsService.updateCorpUserSettings(
                context.getOperationContext(), userUrn, newSettings);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform user home page settings update against input {}, {}",
                input.toString(),
                e.getMessage());
            throw new RuntimeException(
                String.format(
                    "Failed to perform update to user home page settings against input %s",
                    input.toString()),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static void updateCorpUserSettings(
      @Nonnull final CorpUserSettings settings,
      @Nonnull final UpdateUserHomePageSettingsInput input) {
    final CorpUserHomePageSettings newHomePageSettings =
        settings.hasHomePage() ? settings.getHomePage() : new CorpUserHomePageSettings();
    updateCorpUserHomePageSettings(newHomePageSettings, input);
    settings.setHomePage(newHomePageSettings);
  }

  private static void updateCorpUserHomePageSettings(
      @Nonnull final CorpUserHomePageSettings settings,
      @Nonnull final UpdateUserHomePageSettingsInput input) {

    if (input.getPageTemplate() != null) {
      settings.setPageTemplate(UrnUtils.getUrn(input.getPageTemplate()));
    }

    // Append to the list of existing dismissed announcements
    if (input.getNewDismissedAnnouncements() != null) {
      List<String> dismissedAnnouncements =
          settings.hasDismissedAnnouncements()
              ? new ArrayList<>(settings.getDismissedAnnouncements())
              : new ArrayList<>();

      for (String announcement : input.getNewDismissedAnnouncements()) {
        if (!dismissedAnnouncements.contains(announcement)) {
          dismissedAnnouncements.add(announcement);
        }
      }

      settings.setDismissedAnnouncements(new StringArray(dismissedAnnouncements));
    }
  }
}
