package com.linkedin.datahub.graphql.resolvers.settings.group;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@RequiredArgsConstructor
public class UpdateGroupNotificationSettingsResolver implements DataFetcher<CompletableFuture<NotificationSettings>> {
  private final SettingsService _settingsService;

  public CompletableFuture<NotificationSettings> get(DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final QueryContext context = environment.getContext();
      final Authentication authentication = context.getAuthentication();
      final UpdateGroupNotificationSettingsInput input =
          bindArgument(environment.getArgument("input"), UpdateGroupNotificationSettingsInput.class);
      final String groupUrnString = input.getGroupUrn();
      final NotificationSettingsInput notificationSettingsInput = input.getNotificationSettings();

      try {
        if (!canEditGroupMembers(groupUrnString, context)) {
          throw new RuntimeException(
              String.format("Unauthorized to update notification settings for group %s", groupUrnString));
        }

        final Urn groupUrn = UrnUtils.getUrn(groupUrnString);

        CorpGroupSettings corpGroupSettings = _settingsService.getCorpGroupSettings(groupUrn, authentication);
        if (corpGroupSettings == null) {
          corpGroupSettings = new CorpGroupSettings();
        }

        final com.linkedin.event.notification.settings.NotificationSettings notificationSettings =
            corpGroupSettings.hasNotificationSettings()
                ? corpGroupSettings.getNotificationSettings()
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

        corpGroupSettings.setNotificationSettings(notificationSettings);
        _settingsService.updateCorpGroupSettings(groupUrn, corpGroupSettings, authentication);

        return NotificationSettingsMapper.map(notificationSettings);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to update notification settings for group %s", groupUrnString, e));
      }
    });
  }
}
