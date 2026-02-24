package com.linkedin.datahub.graphql.types.notification.mappers;

import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationScenarioType;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.generated.SlackUser;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.settings.NotificationSetting;
import com.linkedin.settings.NotificationSettingMap;
import com.linkedin.settings.NotificationSettingValue;
import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;

public class NotificationSettingsMapperTest {

  @Test
  public void testMapAllFieldsPresent() {
    // Create an internal NotificationSettings object with all fields populated.
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    // --- Sink Types ---
    input.setSinkTypes(
        new NotificationSinkTypeArray(
            Arrays.asList(
                com.linkedin.event.notification.NotificationSinkType.SLACK,
                com.linkedin.event.notification.NotificationSinkType.EMAIL)));

    // --- Slack Settings ---
    com.linkedin.event.notification.settings.SlackNotificationSettings internalSlack =
        new com.linkedin.event.notification.settings.SlackNotificationSettings();
    internalSlack.setUserHandle("testUser");
    internalSlack.setChannels(new StringArray(Arrays.asList("channel1", "channel2")));

    // Add SlackUser (OAuth-bound user)
    com.linkedin.settings.global.SlackUser internalSlackUser =
        new com.linkedin.settings.global.SlackUser();
    internalSlackUser.setSlackUserId("U12345678");
    internalSlackUser.setEmail("slack@example.com");
    internalSlackUser.setDisplayName("Slack User");
    internalSlackUser.setLastUpdated(1609459200000L);
    internalSlack.setUser(internalSlackUser);

    input.setSlackSettings(internalSlack);

    // --- Email Settings ---
    com.linkedin.event.notification.settings.EmailNotificationSettings internalEmail =
        new com.linkedin.event.notification.settings.EmailNotificationSettings();
    internalEmail.setEmail("test@example.com");
    input.setEmailSettings(internalEmail);

    // --- Notification Settings Map ---
    NotificationSettingMap settingsMap = new NotificationSettingMap();
    NotificationSetting internalSetting = new NotificationSetting();
    internalSetting.setValue(NotificationSettingValue.ENABLED);
    settingsMap.put("DATASET_SCHEMA_CHANGE", internalSetting);
    input.setSettings(settingsMap);

    // Call the mapper
    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    // Validate sink types mapping.
    Assert.assertNotNull(output.getSinkTypes());
    Assert.assertEquals(output.getSinkTypes().size(), 2);
    Assert.assertTrue(output.getSinkTypes().contains(NotificationSinkType.SLACK));
    Assert.assertTrue(output.getSinkTypes().contains(NotificationSinkType.EMAIL));

    // Validate Slack settings mapping.
    SlackNotificationSettings outputSlack = output.getSlackSettings();
    Assert.assertNotNull(outputSlack);
    Assert.assertEquals(outputSlack.getUserHandle(), "testUser");
    Assert.assertEquals(outputSlack.getChannels(), Arrays.asList("channel1", "channel2"));

    // Validate SlackUser mapping.
    SlackUser outputSlackUser = outputSlack.getUser();
    Assert.assertNotNull(outputSlackUser);
    Assert.assertEquals(outputSlackUser.getSlackUserId(), "U12345678");
    Assert.assertEquals(outputSlackUser.getEmail(), "slack@example.com");
    Assert.assertEquals(outputSlackUser.getDisplayName(), "Slack User");
    Assert.assertEquals(outputSlackUser.getLastUpdated(), Long.valueOf(1609459200000L));

    // Validate Email settings mapping.
    EmailNotificationSettings outputEmail = output.getEmailSettings();
    Assert.assertNotNull(outputEmail);
    Assert.assertEquals(outputEmail.getEmail(), "test@example.com");

    // Validate Notification Settings map mapping.
    Assert.assertNotNull(output.getSettings());
    Assert.assertEquals(output.getSettings().size(), 1);
    Assert.assertEquals(
        output.getSettings().get(0).getType(), NotificationScenarioType.DATASET_SCHEMA_CHANGE);
    Assert.assertEquals(output.getSettings().get(0).getValue().toString(), "ENABLED");
  }

  @Test
  public void testMapNoOptionalFields() {
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSinkTypes());
    Assert.assertTrue(output.getSinkTypes().isEmpty());
    Assert.assertNull(output.getSlackSettings());
    Assert.assertNull(output.getEmailSettings());
    Assert.assertNull(output.getSettings());
  }

  @Test
  public void testMapOnlySinkTypes() {
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();
    input.setSinkTypes(
        new NotificationSinkTypeArray(
            Collections.singletonList(com.linkedin.event.notification.NotificationSinkType.EMAIL)));

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSinkTypes());
    Assert.assertEquals(output.getSinkTypes().size(), 1);
    Assert.assertTrue(output.getSinkTypes().contains(NotificationSinkType.EMAIL));

    Assert.assertNull(output.getSlackSettings());
    Assert.assertNull(output.getEmailSettings());
    Assert.assertNull(output.getSettings());
  }

  @Test
  public void testMapOnlySlackSettings() {
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    com.linkedin.event.notification.settings.SlackNotificationSettings internalSlack =
        new com.linkedin.event.notification.settings.SlackNotificationSettings();
    internalSlack.setUserHandle("testUser");
    internalSlack.setChannels(new StringArray(Arrays.asList("general", "random")));
    input.setSlackSettings(internalSlack);

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSlackSettings());
    Assert.assertEquals(output.getSlackSettings().getUserHandle(), "testUser");
    Assert.assertEquals(
        output.getSlackSettings().getChannels(), Arrays.asList("general", "random"));

    Assert.assertTrue(output.getSinkTypes().isEmpty());
    Assert.assertNull(output.getEmailSettings());
    Assert.assertNull(output.getSettings());
  }

  @Test
  public void testMapOnlyEmailSettings() {
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    com.linkedin.event.notification.settings.EmailNotificationSettings internalEmail =
        new com.linkedin.event.notification.settings.EmailNotificationSettings();
    internalEmail.setEmail("user@example.com");
    input.setEmailSettings(internalEmail);

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getEmailSettings());
    Assert.assertEquals(output.getEmailSettings().getEmail(), "user@example.com");

    Assert.assertTrue(output.getSinkTypes().isEmpty());
    Assert.assertNull(output.getSlackSettings());
    Assert.assertNull(output.getSettings());
  }

  @Test
  public void testMapSlackSettingsWithUserNull() {
    // Test that when SlackUser is null, only legacy fields are mapped
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    com.linkedin.event.notification.settings.SlackNotificationSettings internalSlack =
        new com.linkedin.event.notification.settings.SlackNotificationSettings();
    internalSlack.setUserHandle("legacyHandle");
    internalSlack.setChannels(new StringArray(Arrays.asList("channel1")));
    // Note: user is NOT set (null)
    input.setSlackSettings(internalSlack);

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSlackSettings());
    Assert.assertEquals(output.getSlackSettings().getUserHandle(), "legacyHandle");
    Assert.assertEquals(output.getSlackSettings().getChannels(), Arrays.asList("channel1"));
    // SlackUser should be null when not set
    Assert.assertNull(output.getSlackSettings().getUser());
  }

  @Test
  public void testMapSlackSettingsWithUserAllFields() {
    // Test that SlackUser with all fields is properly mapped
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    com.linkedin.event.notification.settings.SlackNotificationSettings internalSlack =
        new com.linkedin.event.notification.settings.SlackNotificationSettings();

    // Create SlackUser with all fields
    com.linkedin.settings.global.SlackUser internalUser =
        new com.linkedin.settings.global.SlackUser();
    internalUser.setSlackUserId("U12345678");
    internalUser.setEmail("user@example.com");
    internalUser.setDisplayName("John Doe");
    internalUser.setLastUpdated(1234567890L);
    internalSlack.setUser(internalUser);

    // Also set legacy field for backward compatibility
    internalSlack.setUserHandle("U12345678");
    input.setSlackSettings(internalSlack);

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSlackSettings());

    // Verify SlackUser is mapped
    SlackUser outputUser = output.getSlackSettings().getUser();
    Assert.assertNotNull(outputUser);
    Assert.assertEquals(outputUser.getSlackUserId(), "U12345678");
    Assert.assertEquals(outputUser.getEmail(), "user@example.com");
    Assert.assertEquals(outputUser.getDisplayName(), "John Doe");
    Assert.assertEquals(outputUser.getLastUpdated(), Long.valueOf(1234567890L));

    // Verify legacy field is also mapped
    Assert.assertEquals(output.getSlackSettings().getUserHandle(), "U12345678");
  }

  @Test
  public void testMapSlackSettingsWithUserPartialFields() {
    // Test that SlackUser with only required fields is properly mapped
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    com.linkedin.event.notification.settings.SlackNotificationSettings internalSlack =
        new com.linkedin.event.notification.settings.SlackNotificationSettings();

    // Create SlackUser with only slackUserId (minimum required)
    com.linkedin.settings.global.SlackUser internalUser =
        new com.linkedin.settings.global.SlackUser();
    internalUser.setSlackUserId("U87654321");
    // email, displayName, lastUpdated are NOT set
    internalSlack.setUser(internalUser);
    input.setSlackSettings(internalSlack);

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSlackSettings());

    // Verify SlackUser is mapped with only slackUserId
    SlackUser outputUser = output.getSlackSettings().getUser();
    Assert.assertNotNull(outputUser);
    Assert.assertEquals(outputUser.getSlackUserId(), "U87654321");
    Assert.assertNull(outputUser.getEmail());
    Assert.assertNull(outputUser.getDisplayName());
    Assert.assertNull(outputUser.getLastUpdated());

    // Legacy userHandle should be null since we didn't set it
    Assert.assertNull(output.getSlackSettings().getUserHandle());
  }

  @Test
  public void testMapSlackSettingsWithUserOnlyNoLegacy() {
    // Test that SlackUser works without legacy userHandle field
    com.linkedin.event.notification.settings.NotificationSettings input =
        new com.linkedin.event.notification.settings.NotificationSettings();

    com.linkedin.event.notification.settings.SlackNotificationSettings internalSlack =
        new com.linkedin.event.notification.settings.SlackNotificationSettings();

    // Create SlackUser (new OAuth-bound user)
    com.linkedin.settings.global.SlackUser internalUser =
        new com.linkedin.settings.global.SlackUser();
    internalUser.setSlackUserId("UOAUTH123");
    internalUser.setEmail("oauth@example.com");
    internalUser.setDisplayName("OAuth User");
    internalSlack.setUser(internalUser);
    // Note: userHandle is NOT set (modern OAuth flow)
    input.setSlackSettings(internalSlack);

    NotificationSettings output = NotificationSettingsMapper.map(null, input);

    Assert.assertNotNull(output.getSlackSettings());

    // Verify SlackUser is mapped
    SlackUser outputUser = output.getSlackSettings().getUser();
    Assert.assertNotNull(outputUser);
    Assert.assertEquals(outputUser.getSlackUserId(), "UOAUTH123");
    Assert.assertEquals(outputUser.getEmail(), "oauth@example.com");
    Assert.assertEquals(outputUser.getDisplayName(), "OAuth User");

    // Legacy userHandle should be null
    Assert.assertNull(output.getSlackSettings().getUserHandle());
  }
}
