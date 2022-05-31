package com.datahub.notification.provider;

import com.datahub.authentication.Authentication;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.settings.global.GlobalSettingsInfo;
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
        this::getLatestSettings,
        1,
        TimeUnit.MINUTES
    );
  }

  /**
   * Returns an instance of {@link GlobalSettingsInfo} if it can be resolved, null otherwise.
   *
   * These settings are refreshed each minute, so the recommendation is not to cache the setting you retrieve
   * using this method.
   *
   * @throws {@link RuntimeException} if Global Settings are unable to be fetched.
   */
  public GlobalSettingsInfo getGlobalSettings() {
      return _globalSettingsSupplier.get();
  }

  private GlobalSettingsInfo getLatestSettings() {
    try {
      log.debug("Refreshing global settings...");
      final EntityResponse response =
          _entityClient.getV2(
              GLOBAL_SETTINGS_ENTITY_NAME,
              GLOBAL_SETTINGS_URN,
              ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME),
              _systemAuthentication);

      // If there's no global settings, log and return null for now. Could be that bootstrap steps have not yet completed.
      if (response == null || response.getAspects().get(GLOBAL_SETTINGS_INFO_ASPECT_NAME) == null) {
        log.error("Failed to find global settings in GMS!");
        return null;
      }

      log.debug("Successfully refreshed global settings.");
      return new GlobalSettingsInfo(
          response
              .getAspects()
              .get(GLOBAL_SETTINGS_INFO_ASPECT_NAME).getValue()
              .data());

    } catch (Exception e) {
      throw new RuntimeException("Caught exception while attempting to refresh global settings!", e);
    }
  }
}