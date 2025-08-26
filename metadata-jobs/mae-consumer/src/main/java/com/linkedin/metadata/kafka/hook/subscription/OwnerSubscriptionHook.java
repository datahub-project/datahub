package com.linkedin.metadata.kafka.hook.subscription;

import static com.linkedin.metadata.Constants.BUSINESS_OWNER_TYPE_URN;
import static com.linkedin.metadata.Constants.CHART_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CONTAINER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_GROUP_ENTITY_NAME;
import static com.linkedin.metadata.Constants.CORP_USER_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_FLOW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_JOB_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_STEWARD_TYPE_URN;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_FEATURE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_FEATURE_TABLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ML_MODEL_ENTITY_NAME;
import static com.linkedin.metadata.Constants.NOTEBOOK_ENTITY_NAME;
import static com.linkedin.metadata.Constants.OWNERSHIP_ASPECT_NAME;
import static com.linkedin.metadata.Constants.TECHNICAL_OWNER_TYPE_URN;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Owner;
import com.linkedin.common.Ownership;
import com.linkedin.common.urn.Urn;
import com.linkedin.event.notification.NotificationSinkType;
import com.linkedin.event.notification.NotificationSinkTypeArray;
import com.linkedin.event.notification.settings.NotificationSettings;
import com.linkedin.event.notification.settings.SlackNotificationSettings;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.settings.SettingsServiceFactory;
import com.linkedin.gms.factory.subscription.SubscriptionServiceFactory;
import com.linkedin.identity.CorpGroupSettings;
import com.linkedin.identity.CorpUserSettings;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.SettingsService;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.subscription.EntityChangeDetails;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionNotificationConfig;
import com.linkedin.subscription.SubscriptionType;
import com.linkedin.subscription.SubscriptionTypeArray;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * Auto-subscribes owners of an entity to an entity when ownership changes. Attempts to use default
 * notification settings, if present. If not present, uses no notification settings.
 */
@Slf4j
@Component
@Import({
  SubscriptionServiceFactory.class,
  SettingsServiceFactory.class,
  SystemAuthenticationFactory.class
})
public class OwnerSubscriptionHook implements MetadataChangeLogHook {

  private static final Set<String> SUPPORTED_ENTITY_TYPES =
      ImmutableSet.of(
          DATASET_ENTITY_NAME,
          DASHBOARD_ENTITY_NAME,
          CHART_ENTITY_NAME,
          DOMAIN_ENTITY_NAME,
          CONTAINER_ENTITY_NAME,
          DATA_JOB_ENTITY_NAME,
          DATA_FLOW_ENTITY_NAME,
          NOTEBOOK_ENTITY_NAME,
          ML_MODEL_ENTITY_NAME,
          ML_FEATURE_ENTITY_NAME,
          ML_FEATURE_TABLE_ENTITY_NAME);

  @VisibleForTesting
  static final Set<Urn> BUSINESS_OWNER_TYPES =
      ImmutableSet.of(BUSINESS_OWNER_TYPE_URN, DATA_STEWARD_TYPE_URN);

  @VisibleForTesting
  static final Set<EntityChangeType> TECHNICAL_USER_CHANGE_TYPES =
      ImmutableSet.of(
          EntityChangeType.DOCUMENTATION_CHANGE,
          EntityChangeType.OWNER_ADDED,
          EntityChangeType.OWNER_REMOVED,
          EntityChangeType.TAG_ADDED,
          EntityChangeType.TAG_REMOVED,
          EntityChangeType.TAG_PROPOSED,
          EntityChangeType.GLOSSARY_TERM_ADDED,
          EntityChangeType.GLOSSARY_TERM_REMOVED,
          EntityChangeType.GLOSSARY_TERM_PROPOSED,
          EntityChangeType.DEPRECATED,
          EntityChangeType.UNDEPRECATED,
          EntityChangeType.OPERATION_COLUMN_ADDED,
          EntityChangeType.OPERATION_COLUMN_REMOVED,
          EntityChangeType.OPERATION_COLUMN_MODIFIED,
          EntityChangeType.INCIDENT_RAISED,
          EntityChangeType.INCIDENT_RESOLVED,
          EntityChangeType.ASSERTION_FAILED);

  @VisibleForTesting
  static final Set<EntityChangeType> BUSINESS_USER_CHANGE_TYPES =
      ImmutableSet.of(
          EntityChangeType.DOCUMENTATION_CHANGE,
          EntityChangeType.OWNER_ADDED,
          EntityChangeType.OWNER_REMOVED,
          EntityChangeType.TAG_ADDED,
          EntityChangeType.TAG_REMOVED,
          EntityChangeType.TAG_PROPOSED,
          EntityChangeType.GLOSSARY_TERM_ADDED,
          EntityChangeType.GLOSSARY_TERM_REMOVED,
          EntityChangeType.GLOSSARY_TERM_PROPOSED,
          EntityChangeType.INCIDENT_RAISED,
          EntityChangeType.INCIDENT_RESOLVED);

  // Fallback when we cannot determine the set of the owner type (custom ownership).
  @VisibleForTesting
  static final Set<EntityChangeType> DEFAULT_SUBSCRIPTION_CHANGE_TYPES =
      TECHNICAL_USER_CHANGE_TYPES;

  private final SubscriptionService subscriptionService;
  private final SettingsService settingsService;
  private OperationContext systemOperationContext;

  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public OwnerSubscriptionHook(
      @Nonnull final SubscriptionService subscriptionService,
      @Nonnull final SettingsService settingsService,
      @Nonnull @Value("${subscriptions.ownerSubscriptionHook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${subscriptions.ownerSubscriptionHook.consumerGroupSuffix}")
          String consumerGroupSuffix) {
    this.subscriptionService =
        Objects.requireNonNull(subscriptionService, "subscriptionService must not be null");
    this.settingsService =
        Objects.requireNonNull(settingsService, "settingsService must not be null");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public OwnerSubscriptionHook(
      @Nonnull final SubscriptionService subscriptionService,
      @Nonnull final SettingsService settingsService,
      @Nonnull Boolean isEnabled) {
    this(subscriptionService, settingsService, isEnabled, "");
  }

  @Override
  public OwnerSubscriptionHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized Owner Subscription hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return this.isEnabled;
  }

  @Override
  public void invoke(@Nonnull MetadataChangeLog event) {
    if (this.isEnabled && isEligibleForProcessing(event)) {
      log.debug("Found ownership change event. Attempting to auto-subscribe owners to entity.");
      handleOwnerChange(
          event.getEntityUrn(),
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(), event.getAspect().getContentType(), Ownership.class),
          event.hasPreviousAspectValue()
              ? GenericRecordUtils.deserializeAspect(
                  event.getPreviousAspectValue().getValue(),
                  event.getPreviousAspectValue().getContentType(),
                  Ownership.class)
              : null);
    }
  }

  public void handleOwnerChange(
      @Nonnull Urn entityUrn, @Nonnull Ownership newOwnership, @Nullable Ownership prevOwnership) {
    // Subscribe new owners to the asset. If there is a subscription already, leave it in place to
    // avoid overwriting existing user preferences.
    final List<Owner> newOwners = newOwnership.getOwners();
    final Set<Urn> prevOwners =
        prevOwnership != null
            ? prevOwnership.getOwners().stream().map(Owner::getOwner).collect(Collectors.toSet())
            : ImmutableSet.of();
    for (Owner owner : newOwners) {
      if (prevOwners.contains(owner.getOwner())) {
        log.debug(
            "Owner {} is already an owner. Skipping auto-subscription creation.", owner.getOwner());
        continue;
      }
      subscribeOwnerToEntity(entityUrn, owner);
    }
    // Do not handle removing subscriptions when owners are removed. We figure they may still be
    // interested
    // in receiving updates and can go ahead and unsubscribe themselves.
  }

  private void subscribeOwnerToEntity(@Nonnull Urn entityUrn, @Nonnull Owner owner) {
    if (subscriptionService.isActorSubscribed(
        systemOperationContext, entityUrn, owner.getOwner())) {
      log.debug(
          "Owner {} already has a subscription for the entity. Skipping subscription creation.",
          owner.getOwner());
      return;
    }
    log.debug(
        "Owner {} does not have a subscription for an entity they own {}. Creating subscription...",
        entityUrn,
        owner.getOwner());
    final Set<EntityChangeType> entityChangeTypes =
        getSubscriptionChangeTypesForOwnerType(owner.getTypeUrn());
    subscriptionService.createSubscription(
        systemOperationContext,
        owner.getOwner(),
        entityUrn,
        new SubscriptionTypeArray(ImmutableList.of(SubscriptionType.ENTITY_CHANGE)),
        new EntityChangeDetailsArray(getEntityChangeDetailsFromTypes(entityChangeTypes)),
        getNotificationConfig(owner.getOwner()));
  }

  private Set<EntityChangeType> getSubscriptionChangeTypesForOwnerType(@Nullable Urn ownerTypeUrn) {
    if (ownerTypeUrn == null) {
      return DEFAULT_SUBSCRIPTION_CHANGE_TYPES;
    } else if (ownerTypeUrn.equals(TECHNICAL_OWNER_TYPE_URN)) {
      // Technical owners
      return TECHNICAL_USER_CHANGE_TYPES;
    } else if (BUSINESS_OWNER_TYPES.contains(ownerTypeUrn)) {
      // Data stewards and business owners.
      return BUSINESS_USER_CHANGE_TYPES;
    } else {
      // Default - any other ownership type.
      return DEFAULT_SUBSCRIPTION_CHANGE_TYPES;
    }
  }

  private List<EntityChangeDetails> getEntityChangeDetailsFromTypes(
      final Set<EntityChangeType> changeTypes) {
    final List<EntityChangeDetails> entityChangeDetails = new ArrayList<>();
    for (EntityChangeType changeType : changeTypes) {
      entityChangeDetails.add(new EntityChangeDetails().setEntityChangeType(changeType));
    }
    return entityChangeDetails;
  }

  /**
   * Resolve the default notification config for the actor. If the user or group has notifications
   * configured in their settings, this will be used to determine a default notification preference.
   */
  @Nullable
  private SubscriptionNotificationConfig getNotificationConfig(@Nonnull final Urn actorUrn) {
    if (CORP_USER_ENTITY_NAME.equals(actorUrn.getEntityType())) {
      return getUserNotificationConfig(actorUrn);
    } else if (CORP_GROUP_ENTITY_NAME.equals(actorUrn.getEntityType())) {
      return getGroupNotificationConfig(actorUrn);
    } else {
      log.warn(
          "Actor urn must be a user or group urn when retrieving default notification configs. Found urn %s");
      return null;
    }
  }

  @Nullable
  private SubscriptionNotificationConfig getUserNotificationConfig(@Nonnull final Urn userUrn) {
    CorpUserSettings settings =
        this.settingsService.getCorpUserSettings(systemOperationContext, userUrn);
    if (settings == null || !settings.hasNotificationSettings()) {
      return null;
    }
    return getSubscriptionNotificationConfigFromDefaultNotificationSettings(
        settings.getNotificationSettings());
  }

  private SubscriptionNotificationConfig getGroupNotificationConfig(@Nonnull final Urn groupUrn) {
    CorpGroupSettings settings =
        this.settingsService.getCorpGroupSettings(systemOperationContext, groupUrn);
    if (settings == null || !settings.hasNotificationSettings()) {
      return null;
    }
    return getSubscriptionNotificationConfigFromDefaultNotificationSettings(
        settings.getNotificationSettings());
  }

  private SubscriptionNotificationConfig
      getSubscriptionNotificationConfigFromDefaultNotificationSettings(
          @Nonnull final NotificationSettings defaultNotificationSettings) {
    SubscriptionNotificationConfig subscriptionNotificationConfig =
        new SubscriptionNotificationConfig();
    NotificationSettings subscriptionNotificationSettings = new NotificationSettings();
    final List<NotificationSinkType> subscriptionSinkTypes = new ArrayList<>();
    if (defaultNotificationSettings.hasSinkTypes()
        && defaultNotificationSettings.getSinkTypes().contains(NotificationSinkType.SLACK)) {
      // Add slack configs.
      subscriptionSinkTypes.add(NotificationSinkType.SLACK);
      // Frontend requires an empty settings object as opposed to NULL!
      subscriptionNotificationSettings.setSlackSettings(new SlackNotificationSettings());
    }
    if (defaultNotificationSettings.hasSinkTypes()
        && defaultNotificationSettings.getSinkTypes().contains(NotificationSinkType.EMAIL)) {
      subscriptionSinkTypes.add(NotificationSinkType.EMAIL);
    }
    subscriptionNotificationSettings.setSinkTypes(
        new NotificationSinkTypeArray(subscriptionSinkTypes));
    subscriptionNotificationConfig.setNotificationSettings(subscriptionNotificationSettings);
    return subscriptionNotificationConfig;
  }

  /**
   * Returns true if the event should be processed, false otherwise. This hook only processes
   * ownership change events.
   */
  private boolean isEligibleForProcessing(final MetadataChangeLog event) {
    return SUPPORTED_ENTITY_TYPES.contains(event.getEntityType())
        && !event.getChangeType().equals(ChangeType.DELETE)
        && OWNERSHIP_ASPECT_NAME.equals(event.getAspectName());
  }
}
