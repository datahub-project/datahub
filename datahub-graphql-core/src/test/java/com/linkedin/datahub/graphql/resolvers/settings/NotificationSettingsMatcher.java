package com.linkedin.datahub.graphql.resolvers.settings;

import com.linkedin.datahub.graphql.generated.NotificationSettings;
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
    return _expected.getActorUrn().equals(actual.getActorUrn()) && _expected.getActorType()
        .equals(actual.getActorType()) && slackSettingsMatches(actual);
  }

  public boolean slackSettingsMatches(@Nonnull final NotificationSettings actual) {
    final SlackNotificationSettings expectedSlackSettings = _expected.getSlackSettings();
    final SlackNotificationSettings actualSlackSettings = actual.getSlackSettings();
    if (expectedSlackSettings == null && actualSlackSettings == null) {
      return true;
    }

    if (expectedSlackSettings == null ^ actualSlackSettings == null) {
      return false;
    }

    final String actualUserHandle = actualSlackSettings.getUserHandle();
    final String expectedUserHandle = expectedSlackSettings.getUserHandle();
    if (actualUserHandle == null ^ expectedUserHandle == null) {
      return false;
    }

    if (actualUserHandle != null && expectedUserHandle != null && !actualUserHandle.equals(expectedUserHandle)) {
      return false;
    }

    final List<String> actualChannels = actualSlackSettings.getChannels();
    final List<String> expectedChannels = expectedSlackSettings.getChannels();

    if (actualChannels == null ^ expectedChannels == null) {
      return false;
    }

    if (actualChannels != null && expectedChannels != null && (!actualChannels.containsAll(expectedChannels)
        || !expectedChannels.containsAll(actualChannels))) {
      return false;
    }

    return true;
  }
}
