package com.linkedin.datahub.graphql.resolvers.settings.environmentbadge;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateEnvironmentBadgeSettingsInput;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.global.EnvironmentBadgeSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;

/** Resolver responsible for updating the environment badge settings. */
public class UpdateEnvironmentBadgeSettingsResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final SettingsService _settingsService;

  public UpdateEnvironmentBadgeSettingsResolver(@Nonnull final SettingsService settingsService) {
    _settingsService = Objects.requireNonNull(settingsService, "settingsService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateEnvironmentBadgeSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateEnvironmentBadgeSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (AuthorizationUtils.canManageFeatures(context)) {
            try {
              final GlobalSettingsInfo maybeGlobalSettings =
                  _settingsService.getGlobalSettings(context.getOperationContext());
              final GlobalSettingsInfo newGlobalSettings =
                  maybeGlobalSettings != null ? maybeGlobalSettings : new GlobalSettingsInfo();
              newGlobalSettings.setEnvironmentBadge(
                  new EnvironmentBadgeSettings().setEnabled(input.getEnabled()));
              _settingsService.updateGlobalSettings(
                  context.getOperationContext(), newGlobalSettings);
              return true;
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to update environment badge settings! %s", input), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
