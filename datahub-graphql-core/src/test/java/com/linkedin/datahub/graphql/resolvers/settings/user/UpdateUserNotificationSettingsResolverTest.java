package com.linkedin.datahub.graphql.resolvers.settings.user;

import com.datahub.authentication.Authentication;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateUserNotificationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static com.linkedin.metadata.service.SettingsService.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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
    notificationSettings.setSinkTypes(new ArrayList<>());
    input.setNotificationSettings(notificationSettings);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new UpdateUserNotificationSettingsResolver(_settingsService);
  }

  @Test
  public void testCreateSlackNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull())).thenThrow(
        new RuntimeException());

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateUserNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull())).thenReturn(
        USER_SLACK_NOTIFICATION_SETTINGS);
    doThrow(new RuntimeException())
        .when(_settingsService).updateCorpUserSettings(
            eq(USER_URN),
            any(CorpUserSettings.class),
            eq(_authentication));

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateUserNotificationSettingsNewSettings() throws Exception {
    when(_settingsService.getCorpUserSettings(eq(USER_URN), eq(_authentication))).thenReturn(null);
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull())).thenReturn(
        USER_SLACK_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher = new NotificationSettingsMatcher(getMappedUserNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1)).updateCorpUserSettings(
        eq(USER_URN),
        eq(UPDATED_CORP_USER_SETTINGS),
        eq(_authentication));
  }

  @Test
  public void testUpdateUserNotificationSettingsExistingSettings() throws Exception {
    when(_settingsService.getCorpUserSettings(eq(USER_URN), eq(_authentication))).thenReturn(
        DEFAULT_CORP_USER_SETTINGS);
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull())).thenReturn(
        USER_SLACK_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher = new NotificationSettingsMatcher(getMappedUserNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1)).updateCorpUserSettings(
        eq(USER_URN),
        eq(UPDATED_CORP_USER_SETTINGS),
        eq(_authentication));
  }
}
