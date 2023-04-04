package com.linkedin.datahub.graphql.resolvers.settings.group;

import com.datahub.authentication.Authentication;
import com.linkedin.common.ActorType;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettingsInput;
import com.linkedin.datahub.graphql.generated.UpdateGroupNotificationSettingsInput;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.metadata.service.SettingsService;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsTestUtils.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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
    final SlackNotificationSettingsInput slackSettings = new SlackNotificationSettingsInput();
    slackSettings.setChannels(SLACK_CHANNELS);
    input.setSlackSettings(slackSettings);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    _resolver = new UpdateGroupNotificationSettingsResolver(_settingsService);
  }

  @Test
  public void testCreateSlackNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS))).thenThrow(
        new RuntimeException(""));

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateGroupNotificationSettingsExceptionThrown() {
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS))).thenReturn(
        GROUP_SLACK_NOTIFICATION_SETTINGS);
    doThrow(new RuntimeException())
        .when(_settingsService).updateCorpGroupSettings(
            eq(GROUP_URN),
            any(CorpGroupSettings.class),
            eq(_authentication));

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdateGroupNotificationNewSettings() throws Exception {
    when(_settingsService.getCorpGroupSettings(eq(GROUP_URN), eq(_authentication))).thenReturn(null);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS))).thenReturn(
        GROUP_SLACK_NOTIFICATION_SETTINGS);
    when(_settingsService.createNotificationSettings(eq(GROUP_URN)))
        .thenReturn(new NotificationSettings().setActorUrn(GROUP_URN).setActorType(ActorType.GROUP));

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_settingsService, times(1)).updateCorpGroupSettings(
        eq(GROUP_URN),
        eq(CORP_GROUP_SETTINGS),
        eq(_authentication));
  }

  @Test
  public void testUpdateGroupNotificationExistingSettings() throws Exception {
    when(_settingsService.getCorpGroupSettings(eq(GROUP_URN), eq(_authentication))).thenReturn(CORP_GROUP_SETTINGS);
    when(_settingsService.createSlackNotificationSettings(isNull(), eq(SLACK_CHANNELS))).thenReturn(
        GROUP_SLACK_NOTIFICATION_SETTINGS);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_settingsService, never()).createNotificationSettings(eq(GROUP_URN));
    verify(_settingsService, times(1)).updateCorpGroupSettings(
        eq(GROUP_URN),
        eq(CORP_GROUP_SETTINGS),
        eq(_authentication));
  }
}
