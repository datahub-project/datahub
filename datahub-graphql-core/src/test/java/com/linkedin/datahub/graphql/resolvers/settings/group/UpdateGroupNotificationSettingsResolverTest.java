package com.linkedin.datahub.graphql.resolvers.settings.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateGroupNotificationSettingsResolverTest {
  private SettingsService _settingsService;
  private UpdateGroupNotificationSettingsResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    _settingsService = mock(SettingsService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    final UpdateGroupNotificationSettingsInput input = new UpdateGroupNotificationSettingsInput();
    input.setGroupUrn(GROUP_URN_STRING);
    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();
    final SlackNotificationSettingsInput slackSettings = new SlackNotificationSettingsInput();
    slackSettings.setChannels(SLACK_CHANNELS);
    notificationSettings.setSlackSettings(slackSettings);
    input.setNotificationSettings(notificationSettings);
    final EmailNotificationSettingsInput emailSettings = new EmailNotificationSettingsInput();
    emailSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailSettings);
    notificationSettings.setSinkTypes(
        new ArrayList<>(ImmutableList.of(NotificationSinkType.EMAIL, NotificationSinkType.SLACK)));
    input.setNotificationSettings(notificationSettings);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new UpdateGroupNotificationSettingsResolver(_settingsService);
  }

  @Test
  public void testCreateSlackNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenThrow(new RuntimeException(""));
    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateGroupNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenReturn(GROUP_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(GROUP_EMAIL_NOTIFICATION_SETTINGS);
    doThrow(new RuntimeException())
        .when(_settingsService)
        .updateCorpGroupSettings(
            any(OperationContext.class), eq(GROUP_URN), any(CorpGroupSettings.class));

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateGroupNotificationNewSettings() throws Exception {
    when(_settingsService.getCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN)))
        .thenReturn(null);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenReturn(GROUP_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(GROUP_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedGroupNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1))
        .updateCorpGroupSettings(
            any(OperationContext.class), eq(GROUP_URN), eq(CORP_GROUP_SETTINGS));
  }

  @Test
  public void testUpdateGroupNotificationExistingSettings() throws Exception {
    when(_settingsService.getCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN)))
        .thenReturn(CORP_GROUP_SETTINGS);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenReturn(GROUP_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(GROUP_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedGroupNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1))
        .updateCorpGroupSettings(
            any(OperationContext.class), eq(GROUP_URN), eq(CORP_GROUP_SETTINGS));
  }
}
