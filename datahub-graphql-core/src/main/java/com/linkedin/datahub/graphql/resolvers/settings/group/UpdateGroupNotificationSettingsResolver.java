package com.linkedin.datahub.graphql.resolvers.settings.group;

import static com.linkedin.datahub.graphql.authorization.AuthorizationUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingMapMapper;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSettingMap;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class UpdateGroupNotificationSettingsResolver
    implements DataFetcher<CompletableFuture<NotificationSettings>> {
  private final SettingsService _settingsService;

  public CompletableFuture<NotificationSettings> get(DataFetchingEnvironment environment)
      throws Exception {
    return CompletableFuture.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final Authentication authentication = context.getAuthentication();
          final UpdateGroupNotificationSettingsInput input =
              bindArgument(
                  environment.getArgument("input"), UpdateGroupNotificationSettingsInput.class);
          final String groupUrnString = input.getGroupUrn();
          final NotificationSettingsInput notificationSettingsInput =
              input.getNotificationSettings();

          try {
            if (!canManageGroupNotificationSettings(groupUrnString, context)) {
              throw new RuntimeException(
                  String.format(
                      "Unauthorized to update notification settings for group %s", groupUrnString));
            }

            final Urn groupUrn = UrnUtils.getUrn(groupUrnString);

            CorpGroupSettings corpGroupSettings =
                _settingsService.getCorpGroupSettings(context.getOperationContext(), groupUrn);
            if (corpGroupSettings == null) {
              corpGroupSettings = new CorpGroupSettings();
            }

            final com.linkedin.event.notification.settings.NotificationSettings
                notificationSettings =
                    corpGroupSettings.hasNotificationSettings()
                        ? corpGroupSettings.getNotificationSettings()
                        : new com.linkedin.event.notification.settings.NotificationSettings()
                            .setSinkTypes(new NotificationSinkTypeArray());

            if (notificationSettingsInput.getSinkTypes() != null) {
              NotificationSinkTypeArray sinkTypes = new NotificationSinkTypeArray();
              input
                  .getNotificationSettings()
                  .getSinkTypes()
                  .forEach(type -> sinkTypes.add(NotificationSinkType.valueOf(type.toString())));
              notificationSettings.setSinkTypes(sinkTypes);
            }

            final SlackNotificationSettingsInput slackNotificationSettingsInput =
                notificationSettingsInput.getSlackSettings();
            if (slackNotificationSettingsInput != null) {
              final SlackNotificationSettings slackNotificationSettings =
                  _settingsService.createSlackNotificationSettings(
                      slackNotificationSettingsInput.getUserHandle(),
                      slackNotificationSettingsInput.getChannels());
              notificationSettings.setSlackSettings(slackNotificationSettings);
            }

            final EmailNotificationSettingsInput emailNotificationSettingsInput =
                notificationSettingsInput.getEmailSettings();
            if (emailNotificationSettingsInput != null) {
              final EmailNotificationSettings emailNotificationSettings =
                  _settingsService.createEmailNotificationSettings(
                      emailNotificationSettingsInput.getEmail());
              notificationSettings.setEmailSettings(emailNotificationSettings);
            }

            if (input.getNotificationSettings().getSettings() != null) {
              NotificationSettingMap newSettings =
                  NotificationSettingMapMapper.mapNotificationSettingInputList(
                      input.getNotificationSettings().getSettings());
              if (notificationSettings.hasSettings()) {
                // Simply overwrite what's already there.
                notificationSettings.getSettings().putAll(newSettings);
              } else {
                notificationSettings.setSettings(newSettings);
              }
            }

            corpGroupSettings.setNotificationSettings(notificationSettings);
            _settingsService.updateCorpGroupSettings(
                context.getOperationContext(), groupUrn, corpGroupSettings);

            return NotificationSettingsMapper.map(context, notificationSettings);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to update notification settings for group %s", groupUrnString, e));
          }
        });
  }
}
