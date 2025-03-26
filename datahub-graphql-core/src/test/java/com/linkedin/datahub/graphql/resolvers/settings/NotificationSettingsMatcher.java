package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.datahub.graphql.generated.EmailNotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSetting;
import com.linkedin.datahub.graphql.generated.NotificationSettingValue;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.NotificationSinkType;
import com.linkedin.datahub.graphql.generated.SlackNotificationSettings;
import com.linkedin.datahub.graphql.generated.StringMapEntry;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

    final List<NotificationSetting> expectedSettings = _expected.getSettings();
    final List<NotificationSetting> actualSettings = actual.getSettings();

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

    if (expectedSettings == null ^ actualSettings == null) {
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

    // Notification Settings Comparisons
    if (!settingsMatch(expectedSettings, actualSettings)) {
      return false;
    }

    return true;
  }

  private boolean slackSettingsMatch(
      @Nullable final SlackNotificationSettings expectedSlackSettings,
      @Nullable final SlackNotificationSettings actualSlackSettings) {
    if (expectedSlackSettings == null && actualSlackSettings == null) {
      return true;
    }

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
      @Nullable final EmailNotificationSettings expectedEmailSettings,
      @Nullable final EmailNotificationSettings actualEmailSettings) {
    if (expectedEmailSettings == null && actualEmailSettings == null) {
      return true;
    }

    final String actualEmail = actualEmailSettings.getEmail();
    final String expectedEmail = expectedEmailSettings.getEmail();
    if (actualEmail == null ^ expectedEmail == null) {
      return false;
    }

    return actualEmail == null || actualEmail.equals(expectedEmail);
  }

  private boolean settingsMatch(
      @Nullable final List<NotificationSetting> expectedSettings,
      @Nullable final List<NotificationSetting> actualSettings) {
    if (expectedSettings == null && actualSettings == null) {
      return true;
    }

    if (expectedSettings.size() != actualSettings.size()) {
      return false;
    }

    for (int i = 0; i < expectedSettings.size(); i++) {
      final NotificationSetting expectedSetting = expectedSettings.get(i);

      // Because we convert from an unordered map, we need to find the corresponding actualSetting
      // based on the type of the expectedSetting
      final NotificationSetting actualSetting =
          actualSettings.stream()
              .filter(setting -> setting.getType().equals(expectedSetting.getType()))
              .findFirst()
              .orElse(null);

      if (!settingMatch(expectedSetting, actualSetting)) {
        return false;
      }
    }

    return true;
  }

  private boolean settingMatch(
      @Nonnull final NotificationSetting expectedSetting,
      @Nonnull final NotificationSetting actualSetting) {
    if (expectedSetting.getType() != actualSetting.getType()) {
      return false;
    }

    NotificationSettingValue expectedValue = expectedSetting.getValue();
    NotificationSettingValue actualValue = actualSetting.getValue();

    if (!expectedValue.equals(actualValue)) {
      return false;
    }

    List<StringMapEntry> expectedParams = expectedSetting.getParams();
    List<StringMapEntry> actualParams = actualSetting.getParams();

    if (expectedParams == null && actualParams == null) {
      return true;
    }

    if (expectedParams.size() != actualParams.size()) {
      return false;
    }

    for (int i = 0; i < expectedParams.size(); i++) {

      StringMapEntry expectedParam = expectedParams.get(i);

      // Because we convert from an unordered map, we need to find the corresponding actualParam
      // based on the key of the expectedParam
      StringMapEntry actualParam =
          actualParams.stream()
              .filter(param -> param.getKey().equals(expectedParam.getKey()))
              .findFirst()
              .orElse(null);

      if (!expectedParam.getKey().equals(actualParam.getKey())) {
        return false;
      }

      if (!expectedParam.getValue().equals(actualParam.getValue())) {
        return false;
      }
    }

    return true;
  }
}
