package com.linkedin.metadata.kafka.hook.notification.settings;

import static com.linkedin.metadata.Constants.CORP_GROUP_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_EDITABLE_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_INFO_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.notification.NotificationScenarioType;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.EmailNotificationSettings;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.identity.CorpGroupEditableInfo;
import com.linkedin.identity.CorpGroupInfo;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserAppearanceSettings;
import com.linkedin.identity.CorpUserEditableInfo;
import com.linkedin.identity.CorpUserInfo;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import io.datahubproject.metadata.context.OperationContext;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DefaultNotificationSettingsHookTest {

  private static final String TEST_EMAIL = "test@test.com";
  private static final String TEST_SLACK = "slack";
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final Urn TEST_GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");

  @Mock private SettingsService settingsService;

  private DefaultNotificationSettingsHook hook;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    hook = new DefaultNotificationSettingsHook(settingsService, true);
  }

  @Test
  public void testInitialization() {
    assertTrue(hook.isEnabled());
  }

  @Test
  public void testHandleUserUpdateWithNoEmail() {
    MetadataChangeLog event = createMockUserInfoEventWithNoEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpUserSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpUserSettings.class));

    event = createMockUserEditableEventWithNoEmailNorSlack();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpUserSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpUserSettings.class));
  }

  @Test
  public void testDefaultNotificationSettingsAppliedToNewUser() {
    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(null);

    MetadataChangeLog event = createMockUserInfoEventWithEmail();
    hook.invoke(event);
    verify(settingsService, times(1))
        .updateCorpUserSettings(
            nullable(OperationContext.class),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(getMockUserSettings()));

    event = createMockUserEditableInfoEventWithEmail();
    hook.invoke(event);
    verify(settingsService, times(2))
        .updateCorpUserSettings(
            nullable(OperationContext.class),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(getMockUserSettings()));
  }

  @Test
  public void testExistingUserNotificationSettingsNotOverwritten() {
    CorpUserSettings existingSettings =
        new CorpUserSettings().setNotificationSettings(new NotificationSettings());
    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(existingSettings);

    MetadataChangeLog event = createMockUserInfoEventWithEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpUserSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpUserSettings.class));

    event = createMockUserEditableInfoEventWithEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpUserSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpUserSettings.class));
  }

  @Test
  public void testExistingUserNotificationSlackSettingsNotOverwritten() {
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setNotificationSettings(
                new NotificationSettings()
                    .setSlackSettings(new SlackNotificationSettings().setUserHandle(TEST_SLACK)));
    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(existingSettings);

    MetadataChangeLog event = createMockUserEditableInfoEventWithSlack();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpUserSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpUserSettings.class));
  }

  @Test
  public void testExistingUserNotificationWithoutSlackSettingsUpdated() {
    final CorpUserSettings existingSettings = getMockUserSettings();
    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(existingSettings);

    final MetadataChangeLog event = createMockUserEditableInfoEventWithSlack();
    hook.invoke(event);

    verify(settingsService, times(1))
        .updateCorpUserSettings(
            nullable(OperationContext.class),
            Mockito.eq(TEST_USER_URN),
            Mockito.eq(getMockUserSettingsWithEmailAndSlack()));
  }

  @Test
  public void testHandleGroupUpdateWithNoEmail() {
    MetadataChangeLog event = createMockGroupInfoEventWithNoEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpGroupSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpGroupSettings.class));

    event = createMockGroupEditableInfoEventWithNoEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpGroupSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpGroupSettings.class));
  }

  @Test
  public void testDefaultNotificationSettingsAppliedToNewGroup() {
    when(settingsService.getCorpGroupSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(null);

    MetadataChangeLog event = createMockGroupInfoEventWithEmail();
    hook.invoke(event);
    verify(settingsService, times(1))
        .updateCorpGroupSettings(
            nullable(OperationContext.class),
            Mockito.eq(TEST_GROUP_URN),
            Mockito.eq(getMockGroupSettings()));

    event = createMockGroupEditableEventWithEmail();
    hook.invoke(event);
    verify(settingsService, times(2))
        .updateCorpGroupSettings(
            nullable(OperationContext.class),
            Mockito.eq(TEST_GROUP_URN),
            Mockito.eq(getMockGroupSettings()));
  }

  @Test
  public void testExistingGroupNotificationSettingsNotOverwritten() {
    CorpGroupSettings existingSettings =
        new CorpGroupSettings().setNotificationSettings(new NotificationSettings());
    when(settingsService.getCorpGroupSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(existingSettings);

    MetadataChangeLog event = createMockGroupInfoEventWithEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpGroupSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpGroupSettings.class));

    event = createMockGroupEditableEventWithEmail();
    hook.invoke(event);
    verify(settingsService, never())
        .updateCorpGroupSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpGroupSettings.class));
  }

  @Test
  public void testNotInvokedForOtherAspects() {
    MetadataChangeLog event =
        buildMetadataChangeLog(
            TEST_USER_URN,
            "otherAspect",
            ChangeType.UPSERT,
            new CorpUserInfo().setEmail(TEST_EMAIL));

    hook.invoke(event);
    verify(settingsService, never())
        .getCorpUserSettings(nullable(OperationContext.class), any(Urn.class));
    verify(settingsService, never())
        .getCorpGroupSettings(nullable(OperationContext.class), any(Urn.class));
    verify(settingsService, never())
        .updateCorpUserSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpUserSettings.class));
    verify(settingsService, never())
        .updateCorpGroupSettings(
            nullable(OperationContext.class), any(Urn.class), any(CorpGroupSettings.class));
  }

  @Test
  public void testDefaultEmailScenarioSettingsApplied() {
    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(null);

    MetadataChangeLog event = createMockUserInfoEventWithEmail();
    hook.invoke(event);

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    verify(settingsService, times(1))
        .updateCorpUserSettings(
            nullable(OperationContext.class), eq(TEST_USER_URN), settingsCaptor.capture());

    NotificationSettings capturedSettings = settingsCaptor.getValue().getNotificationSettings();
    assertNotNull(capturedSettings.getScenarioSettings());

    // Verify NEW_PROPOSAL scenario
    assertTrue(
        capturedSettings
            .getScenarioSettings()
            .containsKey(NotificationScenarioType.NEW_PROPOSAL.toString()));
    NotificationSetting newProposalSetting =
        capturedSettings
            .getScenarioSettings()
            .get(NotificationScenarioType.NEW_PROPOSAL.toString());
    assertEquals(newProposalSetting.getValue(), NotificationSettingValue.ENABLED);
    assertTrue(newProposalSetting.getParams().get("email.enabled").equals("true"));

    // Verify PROPOSAL_STATUS_CHANGE scenario
    assertTrue(
        capturedSettings
            .getScenarioSettings()
            .containsKey(NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString()));
    NotificationSetting statusChangeSetting =
        capturedSettings
            .getScenarioSettings()
            .get(NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString());
    assertEquals(statusChangeSetting.getValue(), NotificationSettingValue.ENABLED);
    assertTrue(statusChangeSetting.getParams().get("email.enabled").equals("true"));

    // Verify PROPOSER_PROPOSAL_STATUS_CHANGE scenario
    assertTrue(
        capturedSettings
            .getScenarioSettings()
            .containsKey(NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE.toString()));
    NotificationSetting proposerStatusChangeSetting =
        capturedSettings
            .getScenarioSettings()
            .get(NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE.toString());
    assertEquals(proposerStatusChangeSetting.getValue(), NotificationSettingValue.ENABLED);
    assertTrue(proposerStatusChangeSetting.getParams().get("email.enabled").equals("true"));
  }

  @Test
  public void testDefaultSlackScenarioSettingsApplied() {
    CorpUserSettings existingSettings =
        new CorpUserSettings()
            .setNotificationSettings(
                new NotificationSettings()
                    .setEmailSettings(new EmailNotificationSettings().setEmail(TEST_EMAIL)));

    when(settingsService.getCorpUserSettings(nullable(OperationContext.class), any(Urn.class)))
        .thenReturn(existingSettings);

    MetadataChangeLog event = createMockUserEditableInfoEventWithSlack();
    hook.invoke(event);

    ArgumentCaptor<CorpUserSettings> settingsCaptor =
        ArgumentCaptor.forClass(CorpUserSettings.class);
    verify(settingsService, times(1))
        .updateCorpUserSettings(
            nullable(OperationContext.class), eq(TEST_USER_URN), settingsCaptor.capture());

    NotificationSettings capturedSettings = settingsCaptor.getValue().getNotificationSettings();
    assertNotNull(capturedSettings.getScenarioSettings());

    // Verify NEW_PROPOSAL scenario
    assertTrue(
        capturedSettings
            .getScenarioSettings()
            .containsKey(NotificationScenarioType.NEW_PROPOSAL.toString()));
    NotificationSetting newProposalSetting =
        capturedSettings
            .getScenarioSettings()
            .get(NotificationScenarioType.NEW_PROPOSAL.toString());
    assertEquals(newProposalSetting.getValue(), NotificationSettingValue.ENABLED);
    assertTrue(newProposalSetting.getParams().get("slack.enabled").equals("true"));

    // Verify PROPOSAL_STATUS_CHANGE scenario
    assertTrue(
        capturedSettings
            .getScenarioSettings()
            .containsKey(NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString()));
    NotificationSetting statusChangeSetting =
        capturedSettings
            .getScenarioSettings()
            .get(NotificationScenarioType.PROPOSAL_STATUS_CHANGE.toString());
    assertEquals(statusChangeSetting.getValue(), NotificationSettingValue.ENABLED);
    assertTrue(statusChangeSetting.getParams().get("slack.enabled").equals("true"));

    // Verify PROPOSER_PROPOSAL_STATUS_CHANGE scenario
    assertTrue(
        capturedSettings
            .getScenarioSettings()
            .containsKey(NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE.toString()));
    NotificationSetting proposerStatusChangeSetting =
        capturedSettings
            .getScenarioSettings()
            .get(NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE.toString());
    assertEquals(proposerStatusChangeSetting.getValue(), NotificationSettingValue.ENABLED);
    assertTrue(proposerStatusChangeSetting.getParams().get("slack.enabled").equals("true"));
  }

  private CorpUserSettings getMockUserSettings() {
    NotificationSettings notificationSettings = new NotificationSettings();
    notificationSettings.setSinkTypes(
        new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.EMAIL)));
    notificationSettings.setEmailSettings(new EmailNotificationSettings().setEmail(TEST_EMAIL));
    notificationSettings.setScenarioSettings(
        new NotificationSettingMap(
            Stream.of(
                    NotificationScenarioType.NEW_PROPOSAL,
                    NotificationScenarioType.PROPOSAL_STATUS_CHANGE,
                    NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE)
                .collect(
                    Collectors.toMap(
                        NotificationScenarioType::toString,
                        scenarioType ->
                            new NotificationSetting()
                                .setValue(NotificationSettingValue.ENABLED)
                                .setParams(
                                    new StringMap(
                                        Stream.of("email.enabled")
                                            .collect(
                                                Collectors.toMap(
                                                    param -> param, param -> "true"))))))));
    return new CorpUserSettings()
        .setNotificationSettings(notificationSettings)
        .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));
  }

  private CorpUserSettings getMockUserSettingsWithEmailAndSlack() {
    NotificationSettings notificationSettings = new NotificationSettings();
    notificationSettings.setSinkTypes(
        new NotificationSinkTypeArray(
            Stream.of(NotificationSinkType.EMAIL, NotificationSinkType.SLACK)
                .sorted()
                .collect(Collectors.toList())));
    notificationSettings.setEmailSettings(new EmailNotificationSettings().setEmail(TEST_EMAIL));
    notificationSettings.setSlackSettings(
        new SlackNotificationSettings().setUserHandle(TEST_SLACK));
    notificationSettings.setScenarioSettings(
        new NotificationSettingMap(
            Stream.of(
                    NotificationScenarioType.NEW_PROPOSAL,
                    NotificationScenarioType.PROPOSAL_STATUS_CHANGE,
                    NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE)
                .collect(
                    Collectors.toMap(
                        NotificationScenarioType::toString,
                        scenarioType ->
                            new NotificationSetting()
                                .setValue(NotificationSettingValue.ENABLED)
                                .setParams(
                                    new StringMap(
                                        Stream.of("email.enabled")
                                            .collect(
                                                Collectors.toMap(
                                                    param -> param, param -> "true"))))))));
    return new CorpUserSettings()
        .setNotificationSettings(notificationSettings)
        .setAppearance(new CorpUserAppearanceSettings().setShowSimplifiedHomepage(false));
  }

  private CorpGroupSettings getMockGroupSettings() {
    NotificationSettings notificationSettings = new NotificationSettings();
    notificationSettings.setSinkTypes(
        new NotificationSinkTypeArray(ImmutableList.of(NotificationSinkType.EMAIL)));
    notificationSettings.setEmailSettings(new EmailNotificationSettings().setEmail(TEST_EMAIL));
    notificationSettings.setScenarioSettings(
        new NotificationSettingMap(
            Stream.of(
                    NotificationScenarioType.NEW_PROPOSAL,
                    NotificationScenarioType.PROPOSAL_STATUS_CHANGE,
                    NotificationScenarioType.PROPOSER_PROPOSAL_STATUS_CHANGE)
                .collect(
                    Collectors.toMap(
                        NotificationScenarioType::toString,
                        scenarioType ->
                            new NotificationSetting()
                                .setValue(NotificationSettingValue.ENABLED)
                                .setParams(
                                    new StringMap(
                                        Stream.of("email.enabled")
                                            .collect(
                                                Collectors.toMap(
                                                    param -> param, param -> "true"))))))));
    return new CorpGroupSettings().setNotificationSettings(notificationSettings);
  }

  // Helper methods to create mock events and conditions
  private MetadataChangeLog createMockUserInfoEventWithNoEmail() {
    return buildMetadataChangeLog(
        TEST_USER_URN, CORP_USER_INFO_ASPECT_NAME, ChangeType.UPSERT, new CorpUserInfo());
  }

  private MetadataChangeLog createMockUserEditableEventWithNoEmailNorSlack() {
    return buildMetadataChangeLog(
        TEST_USER_URN,
        CORP_USER_EDITABLE_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpUserEditableInfo());
  }

  private MetadataChangeLog createMockUserInfoEventWithEmail() {
    return buildMetadataChangeLog(
        TEST_USER_URN,
        CORP_USER_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpUserInfo().setEmail(TEST_EMAIL));
  }

  private MetadataChangeLog createMockUserEditableInfoEventWithEmail() {
    return buildMetadataChangeLog(
        TEST_USER_URN,
        CORP_USER_EDITABLE_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpUserEditableInfo().setEmail(TEST_EMAIL));
  }

  private MetadataChangeLog createMockUserEditableInfoEventWithSlack() {
    return buildMetadataChangeLog(
        TEST_USER_URN,
        CORP_USER_EDITABLE_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpUserEditableInfo().setSlack(TEST_SLACK));
  }

  private MetadataChangeLog createMockGroupInfoEventWithNoEmail() {
    return buildMetadataChangeLog(
        TEST_GROUP_URN, CORP_GROUP_INFO_ASPECT_NAME, ChangeType.UPSERT, new CorpUserInfo());
  }

  private MetadataChangeLog createMockGroupEditableInfoEventWithNoEmail() {
    return buildMetadataChangeLog(
        TEST_GROUP_URN,
        CORP_GROUP_EDITABLE_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpGroupEditableInfo());
  }

  private MetadataChangeLog createMockGroupInfoEventWithEmail() {
    return buildMetadataChangeLog(
        TEST_GROUP_URN,
        CORP_GROUP_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpGroupInfo().setEmail(TEST_EMAIL));
  }

  private MetadataChangeLog createMockGroupEditableEventWithEmail() {
    return buildMetadataChangeLog(
        TEST_GROUP_URN,
        CORP_GROUP_EDITABLE_INFO_ASPECT_NAME,
        ChangeType.UPSERT,
        new CorpGroupEditableInfo().setEmail(TEST_EMAIL));
  }

  private MetadataChangeLog buildMetadataChangeLog(
      Urn urn, String aspectName, ChangeType changeType, RecordTemplate aspect) {
    MetadataChangeLog event = new MetadataChangeLog();
    event.setEntityUrn(urn);
    event.setEntityType(urn.getEntityType());
    event.setAspectName(aspectName);
    event.setChangeType(changeType);
    if (aspect != null) {
      event.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    return event;
  }
}
