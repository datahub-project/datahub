package com.linkedin.datahub.graphql.resolvers.settings;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateSampleDataSettingsInput;
import com.linkedin.metadata.service.SampleDataService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Resolver responsible for updating the sample data settings.
 *
 * <p>When toggling sample data off, this resolver will: 1. Update the GlobalSettings to mark sample
 * data as disabled 2. Asynchronously soft-delete all sample data entities
 *
 * <p>When toggling sample data on, this resolver will: 1. Update the GlobalSettings to mark sample
 * data as enabled 2. Asynchronously restore all sample data entities
 */
@Slf4j
public class UpdateSampleDataSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final SampleDataService _sampleDataService;

  public UpdateSampleDataSettingsResolver(@Nonnull final SampleDataService sampleDataService) {
    _sampleDataService =
        Objects.requireNonNull(sampleDataService, "sampleDataService must not be null");
  }

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpdateSampleDataSettingsInput input =
        bindArgument(environment.getArgument("input"), UpdateSampleDataSettingsInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!SettingsMapper.canManageGlobalSettings(context)) {
            throw new AuthorizationException(
                "Unauthorized to perform this action. Please contact your DataHub administrator.");
          }

          try {
            final boolean newEnabledValue = input.getEnabled();
            final boolean currentEnabledValue =
                _sampleDataService.isSampleDataEnabled(context.getOperationContext());

            // Only take action if the value is actually changing
            if (newEnabledValue != currentEnabledValue) {
              log.info(
                  "Attempting to update sample data setting from {} to {}",
                  currentEnabledValue,
                  newEnabledValue);

              // Trigger async soft-delete or restore FIRST
              // If this throws IllegalStateException (operation in progress),
              // we don't update the setting, keeping toggle state accurate
              if (newEnabledValue) {
                log.info("Triggering async restore of sample data entities");
                _sampleDataService.restoreSampleDataAsync(context.getOperationContext());
              } else {
                log.info("Triggering async soft-delete of sample data entities");
                _sampleDataService.softDeleteSampleDataAsync(context.getOperationContext());
              }

              // Update the setting AFTER successfully kicking off async operation
              _sampleDataService.updateSampleDataSetting(
                  context.getOperationContext(), newEnabledValue);
              log.info("Updated sample data setting to {}", newEnabledValue);
            } else {
              log.info("Sample data setting unchanged (already {}), skipping", newEnabledValue);
            }

            return true;
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update sample data settings! %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
