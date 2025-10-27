package com.linkedin.datahub.graphql.resolvers.subscription;

import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityChangeDetails;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfig;
import com.linkedin.datahub.graphql.resolvers.settings.NotificationSettingsMatcher;
import java.util.List;
import java.util.stream.Collectors;
import org.mockito.ArgumentMatcher;

public class DataHubSubscriptionMatcher implements ArgumentMatcher<DataHubSubscription> {

  private final DataHubSubscription _expected;

  public DataHubSubscriptionMatcher(final DataHubSubscription expected) {
    _expected = expected;
  }

  @Override
  public boolean matches(final DataHubSubscription actual) {
    return _expected.getActorUrn().equals(actual.getActorUrn())
        && ((Entity) _expected.getActor()).getUrn().equals(((Entity) actual.getActor()).getUrn())
        && _expected.getSubscriptionUrn().equals(actual.getSubscriptionUrn())
        && _expected.getEntity().getUrn().equals(actual.getEntity().getUrn())
        && listMatches(_expected.getSubscriptionTypes(), actual.getSubscriptionTypes())
        && entityChangeTypesMatches(_expected.getEntityChangeTypes(), actual.getEntityChangeTypes())
        && notificationConfigMatches(
            _expected.getNotificationConfig(), actual.getNotificationConfig());
  }

  private boolean notificationConfigMatches(
      final SubscriptionNotificationConfig expected, final SubscriptionNotificationConfig actual) {

    final NotificationSettings expectedNotificationSettings = expected.getNotificationSettings();
    final NotificationSettings actualNotificationSettings = actual.getNotificationSettings();

    if (expectedNotificationSettings == null && actualNotificationSettings == null) {
      return true;
    }

    if (expectedNotificationSettings == null ^ actualNotificationSettings == null) {
      return false;
    }

    final NotificationSettingsMatcher notificationSettingsMatcher =
        new NotificationSettingsMatcher(expectedNotificationSettings);
    return notificationSettingsMatcher.matches(actualNotificationSettings);
  }

  private boolean entityChangeTypesMatches(
      final List<EntityChangeDetails> expected, final List<EntityChangeDetails> actual) {

    List<EntityChangeType> expectedChangeTypes =
        expected.stream()
            .map(EntityChangeDetails::getEntityChangeType)
            .collect(Collectors.toList());
    List<EntityChangeType> actualChangeTypes =
        actual.stream().map(EntityChangeDetails::getEntityChangeType).collect(Collectors.toList());

    return listMatches(expectedChangeTypes, actualChangeTypes);
  }

  private boolean listMatches(final List<?> expected, final List<?> actual) {
    return expected.containsAll(actual) && actual.containsAll(expected);
  }
}
