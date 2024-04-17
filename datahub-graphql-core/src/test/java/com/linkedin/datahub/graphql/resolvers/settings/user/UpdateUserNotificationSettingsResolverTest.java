package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static com.linkedin.metadata.service.SettingsService.*;
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
import com.linkedin.datahub.graphql.generated.UpdateUserNotificationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateUserNotificationSettingsResolverTest {
  private SettingsService _settingsService;
  private UpdateUserNotificationSettingsResolver _resolver;
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
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    final UpdateUserNotificationSettingsInput input = new UpdateUserNotificationSettingsInput();
    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();
    final SlackNotificationSettingsInput slackSettings = new SlackNotificationSettingsInput();
    slackSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackSettings);
    final EmailNotificationSettingsInput emailSettings = new EmailNotificationSettingsInput();
    emailSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailSettings);
    notificationSettings.setSinkTypes(
        new ArrayList<>(ImmutableList.of(NotificationSinkType.EMAIL, NotificationSinkType.SLACK)));
    input.setNotificationSettings(notificationSettings);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new UpdateUserNotificationSettingsResolver(_settingsService);
  }

  @Test
  public void testCreateSlackNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull()))
        .thenThrow(new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateUserNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull()))
        .thenReturn(USER_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(GROUP_EMAIL_NOTIFICATION_SETTINGS);
    doThrow(new RuntimeException())
        .when(_settingsService)
        .updateCorpUserSettings(
            any(OperationContext.class), eq(USER_URN), any(CorpUserSettings.class));

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateUserNotificationSettingsNewSettings() throws Exception {
    when(_settingsService.getCorpUserSettings(any(OperationContext.class), eq(USER_URN)))
        .thenReturn(null);
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull()))
        .thenReturn(USER_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(USER_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedUserNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1))
        .updateCorpUserSettings(
            any(OperationContext.class), eq(USER_URN), eq(UPDATED_CORP_USER_SETTINGS));
  }

  @Test
  public void testUpdateUserNotificationSettingsExistingSettings() throws Exception {
    when(_settingsService.getCorpUserSettings(any(OperationContext.class), eq(USER_URN)))
        .thenReturn(DEFAULT_CORP_USER_SETTINGS);
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull()))
        .thenReturn(USER_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(USER_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedUserNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1))
        .updateCorpUserSettings(
            any(OperationContext.class), eq(USER_URN), eq(UPDATED_CORP_USER_SETTINGS));
  }
}
