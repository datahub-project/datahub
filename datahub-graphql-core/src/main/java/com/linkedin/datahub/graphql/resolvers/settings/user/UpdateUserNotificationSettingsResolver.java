package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateUserNotificationSettingsInput;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class UpdateUserNotificationSettingsResolver implements DataFetcher<CompletableFuture<NotificationSettings>> {
  private final SettingsService _settingsService;

  public CompletableFuture<NotificationSettings> get(DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final QueryContext context = environment.getContext();
      final Authentication authentication = context.getAuthentication();
      final String userUrnString = context.getActorUrn();
      final UpdateUserNotificationSettingsInput input =
          bindArgument(environment.getArgument("input"), UpdateUserNotificationSettingsInput.class);
      final NotificationSettingsInput notificationSettingsInput = input.getNotificationSettings();

      try {
        final Urn userUrn = UrnUtils.getUrn(userUrnString);

        CorpUserSettings corpUserSettings = _settingsService.getCorpUserSettings(userUrn, authentication);
        if (corpUserSettings == null) {
          corpUserSettings = SettingsService.DEFAULT_CORP_USER_SETTINGS;
        }

        final com.linkedin.event.notification.settings.NotificationSettings notificationSettings =
            corpUserSettings.hasNotificationSettings() ? corpUserSettings.getNotificationSettings()
                : new com.linkedin.event.notification.settings.NotificationSettings();
        NotificationSinkTypeArray sinkTypes = new NotificationSinkTypeArray();
        input.getNotificationSettings().getSinkTypes().forEach(type -> sinkTypes.add(NotificationSinkType.valueOf(type.toString())));
        notificationSettings.setSinkTypes(sinkTypes);

        final SlackNotificationSettingsInput slackNotificationSettingsInput =
            notificationSettingsInput.getSlackSettings();
        // In the future, add blocks for other notification settings.
        if (slackNotificationSettingsInput != null) {
          final SlackNotificationSettings slackNotificationSettings =
              _settingsService.createSlackNotificationSettings(slackNotificationSettingsInput.getUserHandle(),
                  slackNotificationSettingsInput.getChannels());
          notificationSettings.setSlackSettings(slackNotificationSettings);
        }

        corpUserSettings.setNotificationSettings(notificationSettings);
        _settingsService.updateCorpUserSettings(userUrn, corpUserSettings, authentication);

        return NotificationSettingsMapper.map(notificationSettings);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to update notification settings for user %s", userUrnString, e));
      }
    });
  }
}
