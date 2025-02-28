package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import java.util.List;
import javax.annotation.Nonnull;
import org.mockito.ArgumentMatcher;

public class NotificationSettingsMatcher implements ArgumentMatcher<NotificationSettings> {
  private final NotificationSettings _expected;

  public NotificationSettingsMatcher(@Nonnull final NotificationSettings expected) {
    _expected = expected;
  }

  @Override
  public boolean matches(@Nonnull final NotificationSettings actual) {
    return settingsMatch(actual);
  }

  public boolean settingsMatch(@Nonnull final NotificationSettings actual) {

    final SlackNotificationSettings expectedSlackSettings = _expected.getSlackSettings();
    final SlackNotificationSettings actualSlackSettings = actual.getSlackSettings();

    final EmailNotificationSettings expectedEmailSettings = _expected.getEmailSettings();
    final EmailNotificationSettings actualEmailSettings = actual.getEmailSettings();

    final List<NotificationSinkType> expectedSinkTypes = _expected.getSinkTypes();
    final List<NotificationSinkType> actualSinkTypes = actual.getSinkTypes();

    if (expectedSlackSettings == null
        && actualSlackSettings == null
        && expectedEmailSettings == null
        && actualEmailSettings == null
        && expectedSinkTypes == null
        && actualSinkTypes == null) {
      return true;
    }

    if (expectedEmailSettings == null ^ actualEmailSettings == null) {
      return false;
    }

    if (expectedSlackSettings == null ^ actualSlackSettings == null) {
      return false;
    }

    if (expectedSinkTypes == null ^ actualSinkTypes == null) {
      return false;
    }

    if (actualSinkTypes != null
        && expectedSinkTypes != null
        && (!actualSinkTypes.containsAll(expectedSinkTypes)
            || !expectedSinkTypes.containsAll(actualSinkTypes))) {
      return false;
    }

    // Slack Comparisons
    if (!slackSettingsMatch(expectedSlackSettings, actualSlackSettings)) {
      return false;
    }

    // Email Comparisons
    if (!emailSettingsMatch(expectedEmailSettings, actualEmailSettings)) {
      return false;
    }

    return true;
  }

  private boolean slackSettingsMatch(
      @Nonnull final SlackNotificationSettings expectedSlackSettings,
      @Nonnull final SlackNotificationSettings actualSlackSettings) {
    final String actualUserHandle = actualSlackSettings.getUserHandle();
    final String expectedUserHandle = expectedSlackSettings.getUserHandle();
    if (actualUserHandle == null ^ expectedUserHandle == null) {
      return false;
    }

    if (actualUserHandle != null
        && expectedUserHandle != null
        && !actualUserHandle.equals(expectedUserHandle)) {
      return false;
    }

    final List<String> actualChannels = actualSlackSettings.getChannels();
    final List<String> expectedChannels = expectedSlackSettings.getChannels();

    if (actualChannels == null ^ expectedChannels == null) {
      return false;
    }

    if (actualChannels != null
        && expectedChannels != null
        && (!actualChannels.containsAll(expectedChannels)
            || !expectedChannels.containsAll(actualChannels))) {
      return false;
    }
    return true;
  }

  private boolean emailSettingsMatch(
      @Nonnull final EmailNotificationSettings expectedEmailSettings,
      @Nonnull final EmailNotificationSettings actualEmailSettings) {
    final String actualEmail = actualEmailSettings.getEmail();
    final String expectedEmail = expectedEmailSettings.getEmail();
    if (actualEmail == null ^ expectedEmail == null) {
      return false;
    }

    return actualEmail == null || actualEmail.equals(expectedEmail);
  }
}
