package com.linkedin.datahub.graphql.types.subscription.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataHubSubscription;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityChangeDetails;
import com.linkedin.datahub.graphql.generated.EntityChangeDetailsFilter;
import com.linkedin.datahub.graphql.generated.EntityChangeType;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.NotificationSettings;
import com.linkedin.datahub.graphql.generated.SubscriptionNotificationConfig;
import com.linkedin.datahub.graphql.generated.SubscriptionType;
import com.linkedin.datahub.graphql.types.common.mappers.AuditStampMapper;
import com.linkedin.datahub.graphql.types.common.mappers.ResolvedActorMapper;
import com.linkedin.datahub.graphql.types.common.mappers.UrnToEntityMapper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.datahub.graphql.types.notification.mappers.NotificationSettingsMapper;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class DataHubSubscriptionMapper
    implements ModelMapper<Map.Entry<Urn, SubscriptionInfo>, DataHubSubscription> {
  public static final DataHubSubscriptionMapper INSTANCE = new DataHubSubscriptionMapper();
  public static final AuditStampMapper AUDIT_STAMP_MAPPER = new AuditStampMapper();

  public static DataHubSubscription map(
      @Nullable final QueryContext context, final Map.Entry<Urn, SubscriptionInfo> subscription) {
    return INSTANCE.apply(context, subscription);
  }

  @Override
  public DataHubSubscription apply(
      @Nullable final QueryContext context, final Map.Entry<Urn, SubscriptionInfo> subscription) {
    final Urn subscriptionUrn = subscription.getKey();
    final SubscriptionInfo subscriptionInfo = subscription.getValue();
    final DataHubSubscription result = new DataHubSubscription();
    result.setActorUrn(subscriptionInfo.getActorUrn().toString());
    result.setActor(ResolvedActorMapper.map(subscriptionInfo.getActorUrn()));
    result.setSubscriptionUrn(subscriptionUrn.toString());
    // Set Entity interface fields
    result.setUrn(subscriptionUrn.toString());
    result.setType(EntityType.SUBSCRIPTION);
    result.setCreatedOn(AUDIT_STAMP_MAPPER.apply(context, subscriptionInfo.getCreatedOn()));
    result.setUpdatedOn(AUDIT_STAMP_MAPPER.apply(context, subscriptionInfo.getUpdatedOn()));

    final List<SubscriptionType> subscriptionTypes =
        subscriptionInfo.getTypes().stream()
            .map(type -> SubscriptionType.valueOf(type.toString()))
            .collect(Collectors.toList());
    result.setSubscriptionTypes(subscriptionTypes);

    final Entity entity = UrnToEntityMapper.map(context, subscriptionInfo.getEntityUrn());
    result.setEntity(entity);

    final List<EntityChangeDetails> entityChangeTypes =
        subscriptionInfo.hasEntityChangeTypes()
            ? mapEntityChangeDetails(subscriptionInfo.getEntityChangeTypes())
            : Collections.emptyList();
    result.setEntityChangeTypes(entityChangeTypes);

    if (subscriptionInfo.hasNotificationConfig()) {
      final SubscriptionNotificationConfig notificationConfig =
          mapNotificationConfig(context, subscriptionInfo.getNotificationConfig());
      result.setNotificationConfig(notificationConfig);
    }

    return result;
  }

  private SubscriptionNotificationConfig mapNotificationConfig(
      @Nullable final QueryContext context,
      @Nonnull final com.linkedin.subscription.SubscriptionNotificationConfig notificationConfig) {
    final SubscriptionNotificationConfig result = new SubscriptionNotificationConfig();

    if (notificationConfig.hasNotificationSettings()) {
      final NotificationSettings notificationSettings =
          NotificationSettingsMapper.map(context, notificationConfig.getNotificationSettings());
      result.setNotificationSettings(notificationSettings);
    }

    return result;
  }

  private List<EntityChangeDetails> mapEntityChangeDetails(
      @Nonnull final EntityChangeDetailsArray changeDetails) {
    final List<EntityChangeDetails> result = new ArrayList<>();
    for (com.linkedin.subscription.EntityChangeDetails changeDetail : changeDetails) {
      EntityChangeDetails entityChangeDetails = new EntityChangeDetails();
      entityChangeDetails.setEntityChangeType(
          EntityChangeType.valueOf(changeDetail.getEntityChangeType().toString()));
      if (changeDetail.getFilter() != null) {
        entityChangeDetails.setFilter(mapEntityChangeDetailsFilter(changeDetail.getFilter()));
      }
      result.add(entityChangeDetails);
    }

    return result;
  }

  private EntityChangeDetailsFilter mapEntityChangeDetailsFilter(
      @Nonnull final com.linkedin.subscription.EntityChangeDetailsFilter changeDetailsFilter) {
    EntityChangeDetailsFilter result = new EntityChangeDetailsFilter();
    if (changeDetailsFilter.getIncludeAssertions() == null) {
      return result;
    }
    result.setIncludeAssertions(
        changeDetailsFilter.getIncludeAssertions().stream()
            .map(Urn::toString)
            .collect(Collectors.toList()));
    return result;
  }
}
