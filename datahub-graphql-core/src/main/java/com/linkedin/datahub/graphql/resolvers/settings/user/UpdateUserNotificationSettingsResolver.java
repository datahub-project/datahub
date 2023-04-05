package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class UpdateUserNotificationSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final SettingsService _settingsService;

  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final QueryContext context = environment.getContext();
      final Authentication authentication = context.getAuthentication();
      final String userUrnString = context.getActorUrn();
      final UpdateGroupNotificationSettingsInput input =
          bindArgument(environment.getArgument("input"), UpdateGroupNotificationSettingsInput.class);
      final SlackNotificationSettingsInput slackInput = input.getSlackSettings();

      try {
        final Urn userUrn = UrnUtils.getUrn(userUrnString);

        // In the future, add blocks for other notification settings.
        if (slackInput != null) {
          CorpUserSettings corpUserSettings = _settingsService.getCorpUserSettings(userUrn, authentication);
          if (corpUserSettings == null) {
            corpUserSettings = SettingsService.DEFAULT_CORP_USER_SETTINGS;
          }

          final SlackNotificationSettings slackNotificationSettings =
              _settingsService.createSlackNotificationSettings(slackInput.getUserHandle(), slackInput.getChannels());
          final NotificationSettings notificationSettings = corpUserSettings.hasNotifications()
              ? corpUserSettings.getNotifications() : _settingsService.createNotificationSettings(userUrn);
          notificationSettings.setSlackSettings(slackNotificationSettings);
          corpUserSettings.setNotifications(notificationSettings);

          _settingsService.updateCorpUserSettings(userUrn, corpUserSettings, authentication);
        }

        return true;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to update notification settings for user %s", userUrnString, e));
      }
    });
  }
}
