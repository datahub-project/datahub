package com.datahub.notification;

import com.datahub.authentication.Authentication;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.settings.global.GlobalSettingsInfo;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;

@Slf4j
public class SettingsProvider {

  protected final EntityClient _entityClient;
  protected final Authentication _systemAuthentication;
  protected Supplier<GlobalSettingsInfo> _globalSettingsSupplier;

  public SettingsProvider(
      @Nonnull final EntityClient entityClient,
      @Nonnull final Authentication systemAuthentication) {
    _entityClient = entityClient;
    _systemAuthentication = systemAuthentication;
    // Only refetch settings every 1 minutes.
    _globalSettingsSupplier = Suppliers.memoizeWithExpiration(
        this::refreshSettings,
        1,
        TimeUnit.MINUTES
    );
  }

  /**
   * Whether the settings provider is ready to handle requests.
   *
   * If false, the settings provider is not yet initialized.
   */
  public boolean isInitialized() {
    return _globalSettingsSupplier.get() != null;
  }

  /**
   * Returns an instance of {@link GlobalSettingsInfo} if it can be resolved, null otherwise.
   *
   * These settings are refreshed each minute, so the recommendation is not to cache the setting you retrieve
   * using this method.
   */
  public GlobalSettingsInfo getGlobalSettings() {
    return _globalSettingsSupplier.get();
  }

  private GlobalSettingsInfo refreshSettings() {
    try {
      log.debug("Refreshing global settings...");

      final Urn globalSettingsUrn = Urn.createFromTuple(GLOBAL_SETTINGS_ENTITY_NAME, 0);
      final Map<Urn, EntityResponse> response =
          _entityClient.batchGetV2(
              GLOBAL_SETTINGS_ENTITY_NAME,
              ImmutableSet.of(globalSettingsUrn),
              ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME),
              _systemAuthentication);

      // If there's no global settings, log and return null for now. Could be that bootstrap steps have not yet completed.
      if (response.get(globalSettingsUrn) == null || response.get(globalSettingsUrn).getAspects().get(GLOBAL_SETTINGS_INFO_ASPECT_NAME) == null) {
        log.error("Failed to find global settings in GMS!");
        return null;
      }

      log.debug("Successfully refreshed global settings.");
      return new GlobalSettingsInfo(
          response.get(globalSettingsUrn)
              .getAspects()
              .get(GLOBAL_SETTINGS_INFO_ASPECT_NAME).data());

    } catch (Exception e) {
      throw new RuntimeException("Caught exception while attempting to refresh global settings!", e);
    }
  }
}