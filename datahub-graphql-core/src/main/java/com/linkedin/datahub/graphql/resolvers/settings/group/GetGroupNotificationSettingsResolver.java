package com.linkedin.datahub.graphql.resolvers.settings.group;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetGroupNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;


@Slf4j
@RequiredArgsConstructor
public class GetGroupNotificationSettingsResolver implements DataFetcher<CompletableFuture<NotificationSettings>> {
  final SettingsService _settingsService;

  @Override
  public CompletableFuture<NotificationSettings> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();
    final GetGroupNotificationSettingsInput input =
        bindArgument(environment.getArgument("input"), GetGroupNotificationSettingsInput.class);
    final String groupUrnString = input.getGroupUrn();

    return CompletableFuture.supplyAsync(() -> {
      try {
        final Urn groupUrn = UrnUtils.getUrn(groupUrnString);
        final CorpGroupSettings groupSettings = _settingsService.getCorpGroupSettings(groupUrn, authentication);
        if (groupSettings == null || !groupSettings.hasNotificationSettings()) {
          return null;
        }

        return NotificationSettingsMapper.map(groupSettings.getNotificationSettings());
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to get notification settings for group %s", groupUrnString),
            e);
      }
    });
  }
}
