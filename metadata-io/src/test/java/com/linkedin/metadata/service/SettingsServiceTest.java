package com.linkedin.metadata.service;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.identity.CorpUserViewsSettings;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.settings.global.GlobalSettingsInfo;
import com.linkedin.settings.global.GlobalViewsSettings;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

public class SettingsServiceTest {
  private static final String USER_URN_STRING = "urn:li:corpuser:testUser";
  private static final Urn USER_URN = UrnUtils.getUrn(USER_URN_STRING);
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testGroup";
  private static final Urn GROUP_URN = UrnUtils.getUrn(GROUP_URN_STRING);
  private static final Urn TEST_VIEW_URN = UrnUtils.getUrn("urn:li:dataHubView:test");
  private static final String USER_HANDLE = "testUser";
  private static final List<String> CHANNELS = Collections.singletonList("testChannel");
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");

  private static final SlackNotificationSettings USER_SLACK_NOTIFICATION_SETTINGS =
      new SlackNotificationSettings().setUserHandle(USER_HANDLE);
  private static final NotificationSettings USER_NOTIFICATION_SETTINGS = new NotificationSettings()
      .setSlackSettings(USER_SLACK_NOTIFICATION_SETTINGS);
  private static final CorpUserSettings CORP_USER_SETTINGS = new CorpUserSettings()
      .setViews(new CorpUserViewsSettings().setDefaultView(TEST_VIEW_URN))
      .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(true));

  private static final CorpUserSettings UPDATED_CORP_USER_SETTINGS =
      CORP_USER_SETTINGS.setNotificationSettings(USER_NOTIFICATION_SETTINGS);

  private static final EntityResponse CORP_USER_ENTITY_RESPONSE = createEntityResponseFromAspects(
      ImmutableMap.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME, CORP_USER_SETTINGS)
  );
  private static final SlackNotificationSettings GROUP_SLACK_NOTIFICATION_SETTINGS =
      new SlackNotificationSettings().setChannels(new StringArray(CHANNELS));
  private static final NotificationSettings GROUP_NOTIFICATION_SETTINGS = new NotificationSettings()
      .setSlackSettings(GROUP_SLACK_NOTIFICATION_SETTINGS);
  private static final CorpGroupSettings CORP_GROUP_SETTINGS = new CorpGroupSettings()
      .setNotificationSettings(GROUP_NOTIFICATION_SETTINGS);
  private static final CorpGroupSettings UPDATED_CORP_GROUP_SETTINGS = CORP_GROUP_SETTINGS;
  private static final EntityResponse CORP_GROUP_ENTITY_RESPONSE = createEntityResponseFromAspects(
      ImmutableMap.of(Constants.CORP_GROUP_SETTINGS_ASPECT_NAME, CORP_GROUP_SETTINGS)
  );
  private EntityClient _entityClient;
  private SettingsService _settingsService;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);

    _settingsService = new SettingsService(_entityClient, SYSTEM_AUTHENTICATION);
  }

  @Test
  public void testGetCorpUserSettingsMissingActor() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(() -> _settingsService.getCorpUserSettings(USER_URN, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testGetCorpUserSettingsNullSettings() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(
        eq(Constants.CORP_USER_ENTITY_NAME),
        eq(USER_URN),
        eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME)),
        eq(SYSTEM_AUTHENTICATION)
    )).thenReturn(null);

    final CorpUserSettings res = _settingsService.getCorpUserSettings(USER_URN, SYSTEM_AUTHENTICATION);
    assertNull(res);
  }

  @Test
  public void testGetCorpUserSettingsValidSettings() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(
        eq(Constants.CORP_USER_ENTITY_NAME),
        eq(USER_URN),
        eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME)),
        eq(SYSTEM_AUTHENTICATION)
    )).thenReturn(CORP_USER_ENTITY_RESPONSE);

    final CorpUserSettings res =
        _settingsService.getCorpUserSettings(USER_URN, SYSTEM_AUTHENTICATION);
    assertEquals(CORP_USER_SETTINGS, res);
  }

  @Test
  public void testGetCorpUserSettingsException() throws Exception {
    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(
        eq(Constants.CORP_USER_ENTITY_NAME),
        eq(USER_URN),
        eq(ImmutableSet.of(Constants.CORP_USER_SETTINGS_ASPECT_NAME)),
        eq(SYSTEM_AUTHENTICATION)
    )).thenThrow(new RemoteInvocationException());

    assertThrows(RuntimeException.class, () -> _settingsService.getCorpUserSettings(USER_URN, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testGetCorpGroupSettingsMissingActor() throws Exception {
    when(_entityClient.exists(eq(GROUP_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);

    assertThrows(() -> _settingsService.getCorpUserSettings(GROUP_URN, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testGetCorpGroupSettingsNullSettings() throws Exception {
    when(_entityClient.exists(eq(GROUP_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(
        eq(CORP_GROUP_ENTITY_NAME),
        eq(GROUP_URN),
        eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME)),
        eq(SYSTEM_AUTHENTICATION)
    )).thenReturn(null);

    final CorpGroupSettings res = _settingsService.getCorpGroupSettings(GROUP_URN, SYSTEM_AUTHENTICATION);
    Assert.assertNull(res);
  }

  @Test
  public void testGetCorpGroupSettingsValidSettings() throws Exception {
    when(_entityClient.exists(eq(GROUP_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(
        eq(CORP_GROUP_ENTITY_NAME),
        eq(GROUP_URN),
        eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME)),
        eq(SYSTEM_AUTHENTICATION)
    )).thenReturn(CORP_GROUP_ENTITY_RESPONSE);

    final CorpGroupSettings res = _settingsService.getCorpGroupSettings(GROUP_URN, SYSTEM_AUTHENTICATION);
    assertEquals(CORP_GROUP_SETTINGS, res);
  }

  @Test
  public void testGetCorpGroupSettingsException() throws Exception {
    when(_entityClient.exists(eq(GROUP_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.getV2(
        eq(CORP_GROUP_ENTITY_NAME),
        eq(GROUP_URN),
        eq(ImmutableSet.of(CORP_GROUP_SETTINGS_ASPECT_NAME)),
        eq(SYSTEM_AUTHENTICATION)
    )).thenThrow(new RemoteInvocationException());

    assertThrows(RuntimeException.class, () -> _settingsService.getCorpUserSettings(USER_URN, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testUpdateCorpUserSettingsValidSettings() throws Exception {
    final MetadataChangeProposal expectedProposal = buildUpdateCorpUserSettingsChangeProposal(
        USER_URN,
        UPDATED_CORP_USER_SETTINGS
    );

    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.ingestProposal(
        eq(expectedProposal),
        any(Authentication.class),
        eq(false)
    )).thenReturn(USER_URN.toString());

    _settingsService.updateCorpUserSettings(
        USER_URN,
        UPDATED_CORP_USER_SETTINGS,
        SYSTEM_AUTHENTICATION);

    verify(_entityClient, times(1))
        .ingestProposal(
            eq(expectedProposal),
            any(Authentication.class),
            eq(true)
        );
  }

  @Test
  public void testUpdateCorpUserSettingsException() throws Exception {
    final MetadataChangeProposal expectedProposal = buildUpdateCorpUserSettingsChangeProposal(
        USER_URN,
        UPDATED_CORP_USER_SETTINGS
    );

    when(_entityClient.exists(eq(USER_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.ingestProposal(
        eq(expectedProposal),
        any(Authentication.class),
        eq(true)
    )).thenThrow(new RemoteInvocationException());

    assertThrows(RuntimeException.class, () -> _settingsService.updateCorpUserSettings(
        USER_URN,
        UPDATED_CORP_USER_SETTINGS,
        SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testUpdateCorpGroupSettingsValidSettings() throws Exception {
    final MetadataChangeProposal expectedProposal = buildUpdateCorpGroupSettingsChangeProposal(
        GROUP_URN,
        UPDATED_CORP_GROUP_SETTINGS
    );

    when(_entityClient.exists(eq(GROUP_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.ingestProposal(
        eq(expectedProposal),
        any(Authentication.class),
        eq(true)
    )).thenReturn(GROUP_URN.toString());

    _settingsService.updateCorpGroupSettings(
        GROUP_URN,
        UPDATED_CORP_GROUP_SETTINGS,
        SYSTEM_AUTHENTICATION);

    verify(_entityClient, times(1))
        .ingestProposal(
            eq(expectedProposal),
            eq(SYSTEM_AUTHENTICATION),
            eq(true)
        );
  }

  @Test
  public void testUpdateCorpGroupSettingsException() throws Exception {
    final MetadataChangeProposal expectedProposal = buildUpdateCorpGroupSettingsChangeProposal(
        GROUP_URN,
        UPDATED_CORP_GROUP_SETTINGS
    );

    when(_entityClient.exists(eq(GROUP_URN), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    when(_entityClient.ingestProposal(
        eq(expectedProposal),
        eq(SYSTEM_AUTHENTICATION),
        eq(true)
    )).thenThrow(new RemoteInvocationException());

    assertThrows(() -> _settingsService.updateCorpGroupSettings(
        GROUP_URN,
        UPDATED_CORP_GROUP_SETTINGS,
        SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateSlackNotificationSettingsNullInputs() {
    assertThrows(() -> _settingsService.createSlackNotificationSettings(null, null));
  }

  @Test
  public void testCreateSlackNotificationSettingsNullUserHandle() {
    final SlackNotificationSettings slackNotificationSettings =
        _settingsService.createSlackNotificationSettings(null, CHANNELS);
    assertFalse(slackNotificationSettings.hasUserHandle());
    assertTrue(slackNotificationSettings.hasChannels());
    assertEquals(slackNotificationSettings.getChannels(), CHANNELS);
  }

  @Test
  public void testCreateSlackNotificationSettingsNullChannels() {
    final SlackNotificationSettings slackNotificationSettings =
        _settingsService.createSlackNotificationSettings(USER_HANDLE, null);
    assertTrue(slackNotificationSettings.hasUserHandle());
    assertEquals(slackNotificationSettings.getUserHandle(), USER_HANDLE);
    assertFalse(slackNotificationSettings.hasChannels());
  }

  @Test
  public void testGetGlobalSettingsNullSettings() {
    final GlobalSettingsInfo res = _settingsService.getGlobalSettings(SYSTEM_AUTHENTICATION);
    Assert.assertNull(res);
  }

  @Test
  public void testGetGlobalSettingsValidSettings() throws Exception {
    final GlobalSettingsInfo existingSettings = new GlobalSettingsInfo()
        .setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));
    when(_entityClient.getV2(
        eq(GLOBAL_SETTINGS_ENTITY_NAME),
        eq(GLOBAL_SETTINGS_URN),
        eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME)),
        any(Authentication.class)
    )).thenReturn(createEntityResponseFromAspects(
        ImmutableMap.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME, existingSettings)
    ));

    final GlobalSettingsInfo res = _settingsService.getGlobalSettings(SYSTEM_AUTHENTICATION);
    assertEquals(existingSettings, res);
  }

  @Test
  public void testGetGlobalSettingsSettingsException() throws Exception {
    when(_entityClient.getV2(
        eq(GLOBAL_SETTINGS_ENTITY_NAME),
        eq(GLOBAL_SETTINGS_URN),
        eq(ImmutableSet.of(GLOBAL_SETTINGS_INFO_ASPECT_NAME)),
        any(Authentication.class)
    )).thenThrow(new RemoteInvocationException());

    assertThrows(RuntimeException.class, () -> _settingsService.getGlobalSettings(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testUpdateGlobalSettingsValidSettings() throws Exception {
    final GlobalSettingsInfo newSettings = new GlobalSettingsInfo()
        .setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final MetadataChangeProposal expectedProposal = buildUpdateGlobalSettingsChangeProposal(newSettings);

    when(_entityClient.ingestProposal(
        eq(expectedProposal),
        any(Authentication.class),
        eq(false)
    )).thenReturn(GLOBAL_SETTINGS_URN.toString());

    _settingsService.updateGlobalSettings(
        newSettings,
        SYSTEM_AUTHENTICATION);

    verify(_entityClient, times(1))
        .ingestProposal(
            eq(expectedProposal),
            any(Authentication.class),
            eq(false)
        );
  }

  @Test
  public void testUpdateGlobalSettingsSettingsException() throws Exception {
    final GlobalSettingsInfo newSettings = new GlobalSettingsInfo()
        .setViews(new GlobalViewsSettings().setDefaultView(TEST_VIEW_URN));

    final MetadataChangeProposal expectedProposal = buildUpdateGlobalSettingsChangeProposal(
        newSettings
    );

    when(_entityClient.ingestProposal(
        eq(expectedProposal),
        any(Authentication.class),
        eq(false)
    )).thenThrow(new RemoteInvocationException());

    assertThrows(RuntimeException.class, () -> _settingsService.updateGlobalSettings(
        newSettings,
        SYSTEM_AUTHENTICATION));
  }

  private static MetadataChangeProposal buildUpdateCorpUserSettingsChangeProposal(
      final Urn urn,
      final CorpUserSettings newSettings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_USER_ENTITY_NAME);
    mcp.setAspectName(CORP_USER_SETTINGS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(newSettings));
    return mcp;
  }

  private static MetadataChangeProposal buildUpdateCorpGroupSettingsChangeProposal(
      final Urn urn,
      final CorpGroupSettings newSettings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(urn);
    mcp.setEntityType(CORP_GROUP_ENTITY_NAME);
    mcp.setAspectName(CORP_GROUP_SETTINGS_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(newSettings));
    return mcp;
  }

  private static MetadataChangeProposal buildUpdateGlobalSettingsChangeProposal(
      final GlobalSettingsInfo newSettings) {
    final MetadataChangeProposal mcp = new MetadataChangeProposal();
    mcp.setEntityUrn(GLOBAL_SETTINGS_URN);
    mcp.setEntityType(GLOBAL_SETTINGS_ENTITY_NAME);
    mcp.setAspectName(GLOBAL_SETTINGS_INFO_ASPECT_NAME);
    mcp.setChangeType(ChangeType.UPSERT);
    mcp.setAspect(GenericRecordUtils.serializeAspect(newSettings));
    return mcp;
  }
}
