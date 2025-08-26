package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.bindArgument;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateOrganizationDisplayPreferencesInput;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalVisualSettings;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/** Resolver responsible for updating the help link displayed in the UI */
@Slf4j
public class UpdateOrganizationDisplayPreferencesResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  public UpdateOrganizationDisplayPreferencesResolver(
      @Nonnull final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateOrganizationDisplayPreferencesInput input =
        bindArgument(
            environment.getArgument("input"), UpdateOrganizationDisplayPreferencesInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canManageOrganizationDisplayPreferences(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }
          try {
            final GlobalSettingsInfo maybeGlobalSettings =
                _settingsService.getGlobalSettings(context.getOperationContext());

            final GlobalSettingsInfo newGlobalSettings =
                maybeGlobalSettings != null ? maybeGlobalSettings : new GlobalSettingsInfo();

            final GlobalVisualSettings newGlobalVisualSettings =
                newGlobalSettings.getVisual() != null
                    ? newGlobalSettings.getVisual()
                    : new GlobalVisualSettings();

            // Next, patch the global visual settings.
            updateVisualSettings(newGlobalVisualSettings, input);
            newGlobalSettings.setVisual(newGlobalVisualSettings);

            // Finally, write back to GMS.
            _settingsService.updateGlobalSettings(context.getOperationContext(), newGlobalSettings);
            return true;
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to update help link! %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private static void updateVisualSettings(
      @Nonnull final GlobalVisualSettings settings,
      @Nonnull final UpdateOrganizationDisplayPreferencesInput input) {
    if (input.getCustomLogoUrl() != null) {
      settings.setCustomLogoUrl(input.getCustomLogoUrl());
    }
    if (input.getCustomOrgName() != null) {
      settings.setCustomOrgName(input.getCustomOrgName());
    }
  }
}
