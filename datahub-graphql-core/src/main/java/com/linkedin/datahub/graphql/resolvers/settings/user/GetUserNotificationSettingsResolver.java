package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetUserNotificationSettingsResolver
    implements DataFetcher<CompletableFuture<NotificationSettings>> {
  final SettingsService _settingsService;

  @Override
  public CompletableFuture<NotificationSettings> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();

    final String userUrnString = context.getActorUrn();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            final Urn userUrn = UrnUtils.getUrn(userUrnString);
            final CorpUserSettings userSettings =
                _settingsService.getCorpUserSettings(context.getOperationContext(), userUrn);

            if (userSettings == null || !userSettings.hasNotificationSettings()) {
              return null;
            }

            final com.linkedin.event.notification.settings.NotificationSettings
                notificationSettings = userSettings.getNotificationSettings();
            return NotificationSettingsMapper.map(context, notificationSettings);
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to get notification settings for user %s", userUrnString), e);
          }
        });
  }
}
