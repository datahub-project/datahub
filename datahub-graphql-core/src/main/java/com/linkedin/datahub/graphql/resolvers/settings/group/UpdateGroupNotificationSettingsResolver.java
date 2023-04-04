package com.linkedin.datahub.graphql.resolvers.settings.group;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.event.notification.settings.NotificationSettings;
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
public class UpdateGroupNotificationSettingsResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final SettingsService _settingsService;

  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    return CompletableFuture.supplyAsync(() -> {
      final QueryContext context = environment.getContext();
      final Authentication authentication = context.getAuthentication();
      final UpdateGroupNotificationSettingsInput input =
          bindArgument(environment.getArgument("input"), UpdateGroupNotificationSettingsInput.class);
      final String groupUrnString = input.getGroupUrn();

      final SlackNotificationSettingsInput slackInput = input.getSlackSettings();

      try {
        if (!canEditGroupMembers(groupUrnString, context)) {
          throw new RuntimeException(
              String.format("Unauthorized to update notification settings for group %s", groupUrnString));
        }

        final Urn groupUrn = UrnUtils.getUrn(groupUrnString);

        // In the future, add blocks for other notification settings.
        if (slackInput != null) {
          CorpGroupSettings corpGroupSettings = _settingsService.getCorpGroupSettings(groupUrn, authentication);
          if (corpGroupSettings == null) {
            corpGroupSettings = new CorpGroupSettings();
          }

          final SlackNotificationSettings slackNotificationSettings =
              _settingsService.createSlackNotificationSettings(slackInput.getUserHandle(), slackInput.getChannels());
          final NotificationSettings notificationSettings = corpGroupSettings.hasNotifications() ?
              corpGroupSettings.getNotifications() : _settingsService.createNotificationSettings(groupUrn);
          notificationSettings.setSlackSettings(slackNotificationSettings);
          corpGroupSettings.setNotifications(notificationSettings);

          _settingsService.updateCorpGroupSettings(groupUrn, corpGroupSettings, authentication);
        }

        return true;
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to update notification settings for group %s", groupUrnString, e));
      }
    });
  }
}
