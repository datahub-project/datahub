package com.linkedin.datahub.graphql.resolvers.settings.group;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
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
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.settings.NotificationSettingMap;
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
        .thenReturn(SHARED_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);
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
        .thenReturn(SHARED_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);

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
        .thenReturn(SHARED_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedGroupNotificationSettings());
    assertTrue(matcher.matches(_resolver.get(_dataFetchingEnvironment).join()));
    verify(_settingsService, times(1))
        .updateCorpGroupSettings(
            any(OperationContext.class), eq(GROUP_URN), eq(CORP_GROUP_SETTINGS));
  }

  @Test
  public void testUpdateGroupNotificationNewSettingsSettingsOnlyNoExistingGroupSettings()
      throws Exception {
    final DataFetchingEnvironment dataFetchingEnvironment = mock(DataFetchingEnvironment.class);

    final QueryContext mockContext = getMockAllowContext();
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    final UpdateGroupNotificationSettingsInput input = new UpdateGroupNotificationSettingsInput();
    input.setGroupUrn(GROUP_URN_STRING);

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

    when(dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new UpdateGroupNotificationSettingsResolver(_settingsService);

    when(_settingsService.getCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN)))
        .thenReturn(null);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS)))
        .thenReturn(SHARED_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createEmailNotificationSettings(eq(EMAIL_ADDRESS)))
        .thenReturn(SHARED_EMAIL_NOTIFICATION_SETTINGS);

    final NotificationSettingsMatcher matcher =
        new NotificationSettingsMatcher(getMappedSettingsOnlyNotificationSettings());

    assertTrue(matcher.matches(_resolver.get(dataFetchingEnvironment).join()));

    verify(_settingsService, times(1))
        .updateCorpGroupSettings(
            any(OperationContext.class), eq(GROUP_URN), eq(CORP_GROUP_SETTINGS_SETTINGS_ONLY));
  }

  @Test
  public void testUpdateGroupNotificationNewSettingsSettingsOnlySomeExistingGroupSettings()
      throws Exception {
    final DataFetchingEnvironment dataFetchingEnvironment = mock(DataFetchingEnvironment.class);

    final QueryContext mockContext = getMockAllowContext();
    when(dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);

    final UpdateGroupNotificationSettingsInput input = new UpdateGroupNotificationSettingsInput();
    input.setGroupUrn(GROUP_URN_STRING);

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

    _resolver = new UpdateGroupNotificationSettingsResolver(_settingsService);

    when(_settingsService.getCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN)))
        .thenReturn(CORP_GROUP_SETTINGS_ALL_SETTINGS);
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

    CorpGroupSettings newSettings =
        new com.linkedin.identity.CorpGroupSettings()
            .setNotificationSettings(newNotificationSettings);

    verify(_settingsService, times(1))
        .updateCorpGroupSettings(any(OperationContext.class), eq(GROUP_URN), eq(newSettings));
  }
}
