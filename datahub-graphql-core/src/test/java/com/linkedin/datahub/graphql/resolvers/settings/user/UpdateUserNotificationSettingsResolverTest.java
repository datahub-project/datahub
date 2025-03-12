package com.linkedin.datahub.graphql.resolvers.settings.user;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static com.linkedin.metadata.service.SettingsService.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSettingInput;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.NotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateUserNotificationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSettingMap;
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
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);
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
        .thenReturn(
            new CorpUserSettings()
                .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false)));
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
  public void testUpdateUserNotificationSettingsNullSinkTypes() throws Exception {
    when(_settingsService.getCorpUserSettings(any(OperationContext.class), eq(USER_URN)))
        .thenReturn(
            new CorpUserSettings()
                .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false)));
    when(_settingsService.createSlackNotificationSettings(eq(SLACK_USER_HANDLE), isNull()))
        .thenReturn(USER_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(USER_EMAIL_NOTIFICATION_SETTINGS);

    final UpdateUserNotificationSettingsInput input = new UpdateUserNotificationSettingsInput();
    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();
    final SlackNotificationSettingsInput slackSettings = new SlackNotificationSettingsInput();
    slackSettings.setUserHandle(SLACK_USER_HANDLE);
    notificationSettings.setSlackSettings(slackSettings);
    final EmailNotificationSettingsInput emailSettings = new EmailNotificationSettingsInput();
    emailSettings.setEmail(EMAIL_ADDRESS);
    notificationSettings.setEmailSettings(emailSettings);

    notificationSettings.setSinkTypes(null); // Null sink types.
    input.setNotificationSettings(notificationSettings);

    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Ensure sink types are empty.
    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedUserNotificationSettingsEmptySinkTypes());

    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
  }

  @Test
  public void testUpdateUserNotificationNewSettingsSettingsOnlyNoExistingUserSettings()
      throws Exception {

    final UpdateUserNotificationSettingsInput input = new UpdateUserNotificationSettingsInput();
    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();

    final NotificationSettingInput setting1 = new NotificationSettingInput();
    setting1.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntryInput("email.enabled", "true"))));
    setting1.setValue(NotificationSettingValue.ENABLED);
    setting1.setType(NotificationScenarioType.PROPOSAL_STATUS_CHANGE);

    final NotificationSettingInput setting2 = new NotificationSettingInput();
    setting2.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntryInput("email.enabled", "false"))));
    setting2.setValue(NotificationSettingValue.DISABLED);
    setting2.setType(NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    notificationSettings.setSettings(new ArrayList<>(ImmutableList.of(setting1, setting2)));

    input.setNotificationSettings(notificationSettings);

    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    when(_settingsService.getCorpUserSettings(any(OperationContext.class), eq(USER_URN)))
        .thenReturn(null);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenReturn(SHARED_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);

    _resolver = new UpdateUserNotificationSettingsResolver(_settingsService);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedSettingsOnlyNotificationSettings());

    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));

    verify(_settingsService, times(1))
        .updateCorpUserSettings(
            any(OperationContext.class), eq(USER_URN), eq(CORP_USER_SETTINGS_SETTINGS_ONLY));
  }

  @Test
  public void testUpdateUserNotificationNewSettingsSettingsOnlySomeExistingUserSettings()
      throws Exception {
    final DataFetchingEnvironment dataFetchingEnvironment = mock(DataFetchingEnvironment.class);

    final QueryContext mockContext = getMockAllowContext();
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(mockContext.getActorUrn()).thenReturn(USER_URN_STRING);

    final UpdateUserNotificationSettingsInput input = new UpdateUserNotificationSettingsInput();

    final NotificationSettingsInput notificationSettings = new NotificationSettingsInput();

    // Case 1: Add a new type setting!
    final NotificationSettingInput setting1 = new NotificationSettingInput();
    setting1.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntryInput("email.enabled", "true"))));
    setting1.setValue(NotificationSettingValue.ENABLED);
    setting1.setType(NotificationScenarioType.ENTITY_DEPRECATION_CHANGE);

    // Case 2: Overwrite an existing setting!
    final NotificationSettingInput setting2 = new NotificationSettingInput();
    setting2.setParams(
        new ArrayList<>(ImmutableList.of(new StringMapEntryInput("email.enabled", "true"))));
    setting2.setValue(NotificationSettingValue.ENABLED);
    setting2.setType(NotificationScenarioType.ASSERTION_STATUS_CHANGE);

    notificationSettings.setSettings(new ArrayList<>(ImmutableList.of(setting1, setting2)));

    input.setNotificationSettings(notificationSettings);

    when(dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new UpdateUserNotificationSettingsResolver(_settingsService);

    when(_settingsService.getCorpUserSettings(any(OperationContext.class), eq(USER_URN)))
        .thenReturn(CORP_USER_SETTINGS_ALL_SETTINGS);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenReturn(SHARED_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedExistingSettingsNotificationSettings());

    assertTrue(matcher.matches(_resolver.get(dataFetchingEnvironment).join()));

    com.linkedin.event.notification.settings.NotificationSettings newNotificationSettings =
        new com.linkedin.event.notification.settings.NotificationSettings()
            .setSlackSettings(SHARED_SLACK_NOTIFICATION_SETTINGS)
            .setEmailSettings(SHARED_EMAIL_NOTIFICATION_SETTINGS)
            .setSinkTypes(
                new NotificationSinkTypeArray(
                    ImmutableList.of(
                        com.linkedin.event.notification.NotificationSinkType.EMAIL,
                        com.linkedin.event.notification.NotificationSinkType.SLACK)))
            .setSettings(
                new NotificationSettingMap(
                    ImmutableMap.of(
                        NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString(),
                        new com.linkedin.settings.NotificationSetting()
                            .setParams(new StringMap(ImmutableMap.of("email.enabled", "true")))
                            .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED),
                        NotificationScenarioType.ASSERTION_STATUS_CHANGE.toString(),
                        new com.linkedin.settings.NotificationSetting()
                            .setParams(new StringMap(ImmutableMap.of("email.enabled", "true")))
                            .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED),
                        NotificationScenarioType.ENTITY_DEPRECATION_CHANGE.toString(),
                        new com.linkedin.settings.NotificationSetting()
                            .setParams(new StringMap(ImmutableMap.of("email.enabled", "true")))
                            .setValue(com.linkedin.settings.NotificationSettingValue.ENABLED))));

    CorpUserSettings newSettings =
        new com.linkedin.identity.CorpUserSettings()
            .setNotificationSettings(newNotificationSettings);

    verify(_settingsService, times(1))
        .updateCorpUserSettings(any(OperationContext.class), eq(USER_URN), eq(newSettings));
  }
}
