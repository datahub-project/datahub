package com.linkedin.metadata.kafka.hook.subscription;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.kafka.hook.common.AssertionUtils.extractAssertionEntities;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.Status;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.gms.factory.assertions.AssertionServiceFactory;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.gms.factory.subscription.SubscriptionServiceFactory;
import com.linkedin.metadata.kafka.hook.HookUtils;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.service.AssertionService;
import com.linkedin.metadata.service.SubscriptionService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.subscription.EntityChangeDetailsArray;
import com.linkedin.subscription.SubscriptionInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/** Auto-deletes assertion subscriptions when an assertion is deleted. */
@Slf4j
@Component
@Import({
  EntityRegistryFactory.class,
  SubscriptionServiceFactory.class,
  AssertionServiceFactory.class,
  SystemAuthenticationFactory.class
})
public class SubscriptionSubResourceDeletionHook implements MetadataChangeLogHook {

  private static final Integer MAX_SUBSCRIPTIONS_TO_PURGE_PER_ENTITY = 5000;
  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(
          ChangeType.UPDATE,
          ChangeType.UPSERT,
          ChangeType.CREATE,
          ChangeType.RESTATE,
          ChangeType.DELETE);
  private final EntityRegistry entityRegistry;
  private final AssertionService assertionService;
  private final SubscriptionService subscriptionService;
  private OperationContext systemOperationContext;
  private final boolean isEnabled;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public SubscriptionSubResourceDeletionHook(
      @Nonnull @Value("${subscriptions.subResourceDeletionHook.enabled:true}") Boolean isEnabled,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final SubscriptionService subscriptionService,
      @Nonnull final AssertionService assertionService,
      @Nonnull @Value("${subscriptions.subResourceDeletionHook.consumerGroupSuffix}")
          String consumerGroupSuffix) {
    this.entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry is required");
    this.subscriptionService =
        Objects.requireNonNull(subscriptionService, "subscriptionService is required");
    this.assertionService =
        Objects.requireNonNull(assertionService, "assertionService is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public SubscriptionSubResourceDeletionHook(
      @Nonnull Boolean isEnabled,
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final SubscriptionService subscriptionService,
      @Nonnull final AssertionService assertionService) {
    this(isEnabled, entityRegistry, subscriptionService, assertionService, "");
  }

  @Override
  public SubscriptionSubResourceDeletionHook init(
      @Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    log.info("Initialized the subscription subresource deletion hook");
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      log.debug(
          "Urn {} received by Assertion Subscription Subresource Deletion Hook.",
          event.getEntityUrn());
      final Urn urn = HookUtils.getUrnFromEvent(event, entityRegistry);
      // Handle the deletion cases.
      if (isAssertionSoftDeleted(event)) {
        handleAssertionSoftDeleted(urn);
      } else if (isAssertionInfoHardDeleted(event)) {
        handleAssertionInfoHardDeleted(event);
      } else if (isAssertionKeyHardDeleted(event)) {
        handleAssertionKeyHardDeleted(event);
      } else if (isAssertionTargetEntityChanged(event)) {
        handleAssertionTargetEntityChanged(urn, event);
      }
    }
  }

  /** Handles an assertion deletion by removing the assertion from subscription. */
  private void handleAssertionSoftDeleted(@Nonnull final Urn assertionUrn) {
    removeAssertionFromSubscriptions(assertionUrn);
  }

  /** Handles an assertion deletion by removing the assertion from subscription. */
  private void handleAssertionInfoHardDeleted(@Nonnull final MetadataChangeLog event) {
    if (event.hasPreviousAspectValue()) {
      final AssertionInfo previousInfo =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              AssertionInfo.class);
      final List<Urn> prevEntityUrns = extractAssertionEntities(previousInfo);
      for (Urn entityUrn : prevEntityUrns) {
        removeAssertionFromSubscriptions(event.getEntityUrn(), entityUrn);
      }
    } else {
      log.warn(
          String.format(
              "Assertion hard deleted without previous aspect value in MCL Event with entityUrn: %s",
              event.getEntityUrn()));
    }
  }

  /** Handles when an assertion was deleted by removing the assertion key (HARD DELETE) */
  private void handleAssertionKeyHardDeleted(@Nonnull final MetadataChangeLog event) {
    // There is no way to retrieve entityUrn in this case,
    // so we must query all subscription aspects with this assertion's urn
    log.debug(
        "Attempting to clean up subscription references for assertion urn {}.",
        event.getEntityUrn());
    removeAssertionFromSubscriptionsWithOnlyAssertionUrn(event.getEntityUrn());
  }

  private void handleAssertionTargetEntityChanged(
      @Nonnull final Urn assertionUrn, @Nonnull final MetadataChangeLog event) {
    final AssertionInfo info =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), AssertionInfo.class);
    final AssertionInfo prevInfo =
        GenericRecordUtils.deserializeAspect(
            event.getPreviousAspectValue().getValue(),
            event.getPreviousAspectValue().getContentType(),
            AssertionInfo.class);

    final List<Urn> prevEntityUrns = extractAssertionEntities(prevInfo);
    final List<Urn> newEntityUrns = extractAssertionEntities(info);
    final List<Urn> removedEntityUrns = new ArrayList<>(prevEntityUrns);
    removedEntityUrns.removeAll(newEntityUrns);

    // Remove the assertion from the previous entity's subscriptions.
    for (Urn entityUrn : removedEntityUrns) {
      removeAssertionFromSubscriptions(assertionUrn, entityUrn);
    }
  }

  private void removeAssertionFromSubscriptions(final Urn assertionUrn) {
    // 1. Fetch assertion info.
    AssertionInfo assertionInfo =
        assertionService.getAssertionInfo(systemOperationContext, assertionUrn);

    // 2. Retrieve associated urns.
    if (assertionInfo != null) {
      final List<Urn> assertionEntities = extractAssertionEntities(assertionInfo);

      if (assertionEntities.size() <= 0) {
        log.warn(
            String.format(
                "Failed to find entities associated with assertion with urn %s. Skipping removing assertion from subscriptions...",
                assertionUrn));
        return;
      }

      // 3. For each urn, resolve the entity assertions aspect and remove from subscriptions.
      for (Urn entityUrn : assertionEntities) {
        removeAssertionFromSubscriptions(assertionUrn, entityUrn);
      }
    } else {
      log.warn(
          String.format(
              "Failed to find assertionInfo aspect for assertion with urn %s. Skipping removing assertion from subscriptions!",
              assertionUrn));
    }
  }

  /** Removes an assertion to the AssertionSummary aspect for a related entity. */
  private void removeAssertionFromSubscriptions(
      @Nonnull final Urn assertionUrn, @Nonnull final Urn entityUrn) {
    // 1. Fetch subscriptions pointing to this assertion
    final Map<Urn, SubscriptionInfo> subscriptionsReferencingAssertion =
        subscriptionService.listEntityAssertionSubscriptions(
            systemOperationContext, entityUrn, assertionUrn, MAX_SUBSCRIPTIONS_TO_PURGE_PER_ENTITY);

    // 2. Update subscriptions to remove the assertion reference
    subscriptionsReferencingAssertion.forEach(
        (subscriptionUrn, subscriptionInfo) ->
            removeAssertionUrnFromSubscriptionInfo(
                subscriptionInfo, subscriptionUrn, assertionUrn));
  }

  /** NOTE: this is more expensive and should only be used for hard deletes */
  private void removeAssertionFromSubscriptionsWithOnlyAssertionUrn(
      @Nonnull final Urn assertionUrn) {
    // 1. Fetch subscriptions pointing to this assertion
    final Map<Urn, SubscriptionInfo> subscriptionsReferencingAssertion =
        subscriptionService.listAssertionSubscriptionsWithoutEntityUrn(
            systemOperationContext, assertionUrn, MAX_SUBSCRIPTIONS_TO_PURGE_PER_ENTITY);

    // 2. Update subscriptions to remove the assertion reference
    subscriptionsReferencingAssertion.forEach(
        (subscriptionUrn, subscriptionInfo) ->
            removeAssertionUrnFromSubscriptionInfo(
                subscriptionInfo, subscriptionUrn, assertionUrn));
  }

  private void removeAssertionUrnFromSubscriptionInfo(
      @Nonnull SubscriptionInfo subscriptionInfo,
      @Nonnull Urn subscriptionUrn,
      @Nonnull Urn assertionUrn) {

    if (!subscriptionInfo.hasEntityChangeTypes()) {
      return;
    }

    // 1 Remove the assertion reference from the subscription
    subscriptionInfo.setEntityChangeTypes(
        new EntityChangeDetailsArray(
            subscriptionInfo.getEntityChangeTypes().stream()
                .map(
                    changeDetails -> {
                      // If no filter, we can skip
                      if (!changeDetails.hasFilter()
                          || !changeDetails.getFilter().hasIncludeAssertions())
                        return changeDetails;

                      // If there's a filter, remove this urn from it
                      UrnArray newAssertionsToInclude =
                          new UrnArray(
                              changeDetails.getFilter().getIncludeAssertions().stream()
                                  .filter(urn -> !urn.equals(assertionUrn))
                                  .collect(Collectors.toSet()));
                      // If this is the last assertion in the list, removing it is effectively the
                      // same as removing the entity type from subscriptions
                      if (newAssertionsToInclude.isEmpty()) {
                        return null;
                      } else {
                        changeDetails.getFilter().setIncludeAssertions(newAssertionsToInclude);
                      }
                      return changeDetails;
                    })
                .filter(Objects::nonNull)
                .collect(Collectors.toSet())));

    // 2 Emit the change back!
    subscriptionService.updateSubscriptionInfo(
        systemOperationContext, subscriptionUrn, subscriptionInfo);
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the
   * assertion status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isAssertionSoftDeleted(event)
        || isAssertionTargetEntityChanged(event)
        || isAssertionInfoHardDeleted(event)
        || isAssertionKeyHardDeleted(event);
  }

  /** Returns true if an assertion is being soft-deleted. */
  private boolean isAssertionSoftDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && isSoftDeletionEvent(event);
  }

  /** Returns true if an assertion is being hard-deleted. */
  private boolean isAssertionInfoHardDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_INFO_ASPECT_NAME.equals(event.getAspectName());
  }

  /** Returns true if an assertion is being hard-deleted. */
  private boolean isAssertionKeyHardDeleted(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType())
        && ASSERTION_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  /** Returns true if an assertion is being updated to point to another entity. */
  private boolean isAssertionTargetEntityChanged(@Nonnull final MetadataChangeLog event) {
    return ASSERTION_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && isAssertionTargetEntityChangeEvent(event);
  }

  private boolean isSoftDeletionEvent(@Nonnull final MetadataChangeLog event) {
    if (STATUS_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      final Status status =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(), event.getAspect().getContentType(), Status.class);
      return status.hasRemoved() && status.isRemoved();
    }
    return false;
  }

  private boolean isAssertionTargetEntityChangeEvent(@Nonnull final MetadataChangeLog event) {
    if (ASSERTION_INFO_ASPECT_NAME.equals(event.getAspectName()) && event.getAspect() != null) {
      // If there is no previous version, this isn't an update to the URN.
      if (event.getPreviousAspectValue() == null) {
        return false;
      }
      final AssertionInfo info =
          GenericRecordUtils.deserializeAspect(
              event.getAspect().getValue(),
              event.getAspect().getContentType(),
              AssertionInfo.class);

      final AssertionInfo prevInfo =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              AssertionInfo.class);
      final List<Urn> prevEntityUrns = extractAssertionEntities(prevInfo);
      final List<Urn> newEntityUrns = extractAssertionEntities(info);
      return !prevEntityUrns.equals(newEntityUrns);
    }
    return false;
  }
}
