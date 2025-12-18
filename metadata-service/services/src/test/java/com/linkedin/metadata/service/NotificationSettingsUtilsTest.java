package com.linkedin.metadata.service;

import static org.testng.Assert.*;

import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import org.testng.annotations.Test;

public class NotificationSettingsUtilsTest {

  private static final String DATA_HUB_COMMUNITY_UPDATES = "DATA_HUB_COMMUNITY_UPDATES";
  private static final String NEW_PROPOSAL = "NEW_PROPOSAL";
  private static final String PROPOSAL_STATUS_CHANGE = "PROPOSAL_STATUS_CHANGE";
  private static final String TEST_EMAIL = "test@test.com";

  @Test
  public void testCreateDefaultEmailNotificationScenarioSettingsWithCommunityUpdatesEnabled() {
    NotificationSettingMap settings =
        NotificationSettingsUtils.createDefaultEmailNotificationScenarioSettings(true);

    assertNotNull(settings);
    assertTrue(settings.containsKey(DATA_HUB_COMMUNITY_UPDATES));
    assertEquals(
        settings.get(DATA_HUB_COMMUNITY_UPDATES).getValue(), NotificationSettingValue.ENABLED);
    assertEquals(settings.get(DATA_HUB_COMMUNITY_UPDATES).getParams().get("email.enabled"), "true");

    // Verify other default scenarios are present
    assertTrue(settings.containsKey(NEW_PROPOSAL));
    assertEquals(settings.get(NEW_PROPOSAL).getValue(), NotificationSettingValue.ENABLED);
    assertTrue(settings.containsKey(PROPOSAL_STATUS_CHANGE));
    assertEquals(settings.get(PROPOSAL_STATUS_CHANGE).getValue(), NotificationSettingValue.ENABLED);
  }

  @Test
  public void testCreateDefaultEmailNotificationScenarioSettingsWithCommunityUpdatesDisabled() {
    NotificationSettingMap settings =
        NotificationSettingsUtils.createDefaultEmailNotificationScenarioSettings(false);

    assertNotNull(settings);
    assertTrue(settings.containsKey(DATA_HUB_COMMUNITY_UPDATES));
    assertEquals(
        settings.get(DATA_HUB_COMMUNITY_UPDATES).getValue(), NotificationSettingValue.DISABLED);
    assertEquals(
        settings.get(DATA_HUB_COMMUNITY_UPDATES).getParams().get("email.enabled"), "false");

    // Verify other default scenarios are still present and enabled
    assertTrue(settings.containsKey(NEW_PROPOSAL));
    assertEquals(settings.get(NEW_PROPOSAL).getValue(), NotificationSettingValue.ENABLED);
    assertTrue(settings.containsKey(PROPOSAL_STATUS_CHANGE));
    assertEquals(settings.get(PROPOSAL_STATUS_CHANGE).getValue(), NotificationSettingValue.ENABLED);
  }

  @Test
  public void testCreateDefaultNotificationSettingsWithEmailAndCommunityUpdatesEnabled() {
    NotificationSettings settings =
        NotificationSettingsUtils.createDefaultNotificationSettingsWithEmail(TEST_EMAIL, true);

    assertNotNull(settings);
    assertNotNull(settings.getEmailSettings());
    assertEquals(settings.getEmailSettings().getEmail(), TEST_EMAIL);

    assertNotNull(settings.getSinkTypes());
    assertTrue(settings.getSinkTypes().contains(NotificationSinkType.EMAIL));

    assertNotNull(settings.getSettings());
    assertTrue(settings.getSettings().containsKey(DATA_HUB_COMMUNITY_UPDATES));
    assertEquals(
        settings.getSettings().get(DATA_HUB_COMMUNITY_UPDATES).getValue(),
        NotificationSettingValue.ENABLED);
  }

  @Test
  public void testCreateDefaultNotificationSettingsWithEmailAndCommunityUpdatesDisabled() {
    NotificationSettings settings =
        NotificationSettingsUtils.createDefaultNotificationSettingsWithEmail(TEST_EMAIL, false);

    assertNotNull(settings);
    assertNotNull(settings.getEmailSettings());
    assertEquals(settings.getEmailSettings().getEmail(), TEST_EMAIL);

    assertNotNull(settings.getSinkTypes());
    assertTrue(settings.getSinkTypes().contains(NotificationSinkType.EMAIL));

    assertNotNull(settings.getSettings());
    assertTrue(settings.getSettings().containsKey(DATA_HUB_COMMUNITY_UPDATES));
    assertEquals(
        settings.getSettings().get(DATA_HUB_COMMUNITY_UPDATES).getValue(),
        NotificationSettingValue.DISABLED);
  }

  @Test
  public void testAllDefaultScenariosArePresent() {
    NotificationSettingMap settings =
        NotificationSettingsUtils.createDefaultEmailNotificationScenarioSettings(true);

    // Verify all expected scenarios are present
    String[] expectedScenarios = {
      "NEW_PROPOSAL",
      "PROPOSAL_STATUS_CHANGE",
      "PROPOSER_PROPOSAL_STATUS_CHANGE",
      "NEW_ACTION_WORKFLOW_FORM_REQUEST",
      "REQUESTER_ACTION_WORKFLOW_FORM_REQUEST_STATUS_CHANGE",
      "DATA_HUB_COMMUNITY_UPDATES"
    };

    for (String scenario : expectedScenarios) {
      assertTrue(
          settings.containsKey(scenario), "Expected scenario " + scenario + " to be present");
      assertNotNull(
          settings.get(scenario).getValue(), "Expected scenario " + scenario + " to have a value");
    }
  }
}
