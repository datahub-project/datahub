package com.datahub.notification;

import com.datahub.authentication.Authentication;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.settings.global.GlobalIncidentsSettings;
import com.linkedin.settings.global.GlobalIntegrationSettings;
import com.linkedin.settings.global.GlobalNotificationSettings;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.SlackIntegrationSettings;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;

@Slf4j
public class SettingsProvider {

  protected final EntityClient _entityClient;
  protected final Authentication _systemAuthentication;

  protected GlobalSettingsInfo _globalSettings;

  public SettingsProvider(
      @Nonnull final EntityClient entityClient,
      @Nonnull final Authentication systemAuthentication) {
    _entityClient = entityClient;
    _systemAuthentication = systemAuthentication;
    final SettingsRefreshRunnable settingsRefreshRunnable = new SettingsRefreshRunnable(
        this,
        entityClient,
        systemAuthentication
    );
    Executors.newScheduledThreadPool(1).scheduleAtFixedRate(settingsRefreshRunnable, 15, 60, TimeUnit.SECONDS);
  }

  /**
   * Whether the settings provider is ready to handle requests.
   *
   * If false, the settings provider is not yet initialized.
   */
  public boolean isInitialized() {
    return _globalSettings != null;
  }

  /**
   * Returns an instance of {@link GlobalSettingsInfo} if it can be resolved, null otherwise.
   */
  public GlobalSettingsInfo getGlobalSettings() {
    return _globalSettings;
  }

  void setGlobalSettings(
      @Nonnull final GlobalSettingsInfo globalSettings
  ) {
    _globalSettings = globalSettings;
  }

  /**
   * A {@link Runnable} used to periodically sync the latest global settings.
   */
  @VisibleForTesting
  static class SettingsRefreshRunnable implements Runnable {

    private final SettingsProvider _notificationSink;
    private final EntityClient _entityClient;
    private final Authentication _systemAuthentication;

    public SettingsRefreshRunnable(
        final SettingsProvider notificationSink,
        final EntityClient entityClient,
        final Authentication systemAuthentication) {
      _notificationSink = notificationSink;
      _entityClient = entityClient;
      _systemAuthentication = systemAuthentication;
    }

    @Override
    public void run() {
      try {
        log.debug("Bootstrapping global settings...");

        // TODO : REPLACE THIS

        final Urn globalSettingsUrn = Urn.createFromTuple(GLOBAL_SETTINGS_ENTITY_NAME, 0);

        final Map<Urn, EntityResponse> response =
            _entityClient.batchGetV2(
                GLOBAL_SETTINGS_ENTITY_NAME,
                ImmutableSet.of(globalSettingsUrn),
                ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME),
                _systemAuthentication);

        // If there's no global settings, we have a problem.
        if (response.get(globalSettingsUrn) == null || response.get(globalSettingsUrn).getAspects().get(GLOBAL_SETTINGS_INFO_ASPECT_NAME) == null) {
          // throw new RuntimeException("Failed to bootstrap global settings: Missing global settings aspect!");
          // TODO: Boot with some default settings for the platform.
          _notificationSink.setGlobalSettings(new GlobalSettingsInfo()
              .setIntegrations(new GlobalIntegrationSettings()
                  .setSlackSettings(new SlackIntegrationSettings()
                      .setEnabled(true)
                  )
              )
              .setNotifications(new GlobalNotificationSettings().setIncidents(new GlobalIncidentsSettings()))
          );
          return;
        }

        final GlobalSettingsInfo globalSettings = new GlobalSettingsInfo(
            response.get(globalSettingsUrn)
                .getAspects()
                .get(GLOBAL_SETTINGS_INFO_ASPECT_NAME).data());
        _notificationSink.setGlobalSettings(globalSettings);
        log.debug("Successfully bootstrapped global settings.");
      } catch (Exception e) {
        log.error("Failed to bootstrap global settings", e);
      }
    }
  }
}