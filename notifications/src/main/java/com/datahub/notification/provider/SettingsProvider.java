package com.datahub.notification.provider;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.settings.global.GlobalSettingsInfo;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public class SettingsProvider {

  protected final SystemEntityClient systemEntityClient;

  public SettingsProvider(@Nonnull final SystemEntityClient entityClient) {
    // cached 1 minute per application.yaml entityClient cache
    systemEntityClient = entityClient;
  }

  /**
   * Returns an instance of {@link GlobalSettingsInfo} if it can be resolved, null otherwise.
   *
   * <p>These settings are refreshed each minute, so the recommendation is not to cache the setting
   * you retrieve using this method.
   *
   * @throws {@link RuntimeException} if Global Settings are unable to be fetched.
   */
  @Nullable
  public GlobalSettingsInfo getGlobalSettings(@Nonnull OperationContext opContext) {
    try {
      final EntityResponse response =
          systemEntityClient.getV2(
              opContext,
              GLOBAL_SETTINGS_ENTITY_NAME,
              GLOBAL_SETTINGS_URN,
              ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME));

      // If there's no global settings, log and return null for now. Could be that bootstrap steps
      // have not yet completed.
      if (response == null || response.getAspects().get(GLOBAL_SETTINGS_INFO_ASPECT_NAME) == null) {
        log.error("Failed to find global settings in GMS!");
        return null;
      }

      log.debug("Successfully refreshed global settings.");
      return new GlobalSettingsInfo(
          response.getAspects().get(GLOBAL_SETTINGS_INFO_ASPECT_NAME).getValue().data());

    } catch (Exception e) {
      throw new RuntimeException(
          "Caught exception while attempting to refresh global settings!", e);
    }
  }
}
