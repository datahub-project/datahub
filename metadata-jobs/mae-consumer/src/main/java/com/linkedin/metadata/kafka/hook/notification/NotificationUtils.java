package com.linkedin.metadata.kafka.hook.notification;

import static com.linkedin.metadata.AcrylConstants.*;
import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;

import com.datahub.notification.provider.EntityNameProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.event.notification.NotificationRecipient;
import com.linkedin.event.notification.NotificationRecipientOriginType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.pegasus2avro.subscription.SubscriptionType;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionInfo;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class NotificationUtils {

  private static final Set<String> INGESTION_EXECUTION_REQUEST_ASPECTS =
      ImmutableSet.of(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);

  /** Given an Entity Urn, generates a relative path from it (for rendering in the UI) */
  @Nonnull
  public static String generateEntityPath(final Urn entityUrn) {
    final String encodedEntityUrn = URLEncoder.encode(entityUrn.toString(), StandardCharsets.UTF_8);
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return String.format("/dataset/%s", encodedEntityUrn);
      case Constants.CHART_ENTITY_NAME:
        return String.format("/chart/%s", encodedEntityUrn);
      case Constants.DASHBOARD_ENTITY_NAME:
        return String.format("/dashboard/%s", encodedEntityUrn);
      case Constants.DATA_FLOW_ENTITY_NAME:
        return String.format("/pipeline/%s", encodedEntityUrn);
      case Constants.DATA_JOB_ENTITY_NAME:
        return String.format("/task/%s", encodedEntityUrn);
      case Constants.TAG_ENTITY_NAME:
        return String.format("/tag/%s", encodedEntityUrn);
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return String.format("/glossaryTerm/%s", encodedEntityUrn);
      case Constants.DOMAIN_ENTITY_NAME:
        return String.format("/domain/%s", encodedEntityUrn);
      case Constants.CONTAINER_ENTITY_NAME:
        return String.format("/container/%s", encodedEntityUrn);
      case Constants.CORP_USER_ENTITY_NAME:
        return String.format("/user/%s", encodedEntityUrn);
      case Constants.CORP_GROUP_ENTITY_NAME:
        return String.format("/group/%s", encodedEntityUrn);
      case Constants.STRUCTURED_PROPERTY_ENTITY_NAME:
        // Currently, there are no URL for specific properties.
        return "/structured-properties";
      default:
        return "";
    }
  }

  @Nonnull
  public static List<String> generateEntityPaths(final List<Urn> entityUrns) {
    return entityUrns.stream()
        .map(NotificationUtils::generateEntityPath)
        .collect(Collectors.toList());
  }

  @Nonnull
  public static SubscriptionInfo mapSubscriptionInfo(@Nonnull final EntityResponse entityResponse) {
    return new SubscriptionInfo(
        entityResponse.getAspects().get(SUBSCRIPTION_INFO_ASPECT_NAME).getValue().data());
  }

  @Nonnull
  public static Filter createSubscriberFilter(
      @Nonnull final Urn entityUrn, @Nonnull final EntityChangeType changeType) {
    final Criterion entityCriterion = buildEntityCriterion(entityUrn);
    final Criterion changeTypeCriterion = buildChangeTypeCriterion(changeType);
    final CriterionArray criterionArray =
        new CriterionArray(ImmutableList.of(entityCriterion, changeTypeCriterion));

    return new Filter()
        .setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criterionArray)));
  }

  @Nonnull
  public static Filter createDownstreamSubscriberFilter(
      @Nonnull final Set<Urn> entityUrns, @Nonnull final EntityChangeType changeType) {
    final Filter filter = new Filter();
    final ConjunctiveCriterionArray disjunction = new ConjunctiveCriterionArray();
    final ConjunctiveCriterion conjunction = new ConjunctiveCriterion();
    final CriterionArray andCriterion = new CriterionArray();

    final Criterion entityCriterion = buildEntitiesCriterion(entityUrns);
    andCriterion.add(entityCriterion);
    final Criterion upstreamCriterion = buildUpstreamCriterion();
    andCriterion.add(upstreamCriterion);
    final Criterion changeTypeCriterion = buildChangeTypeCriterion(changeType);
    andCriterion.add(changeTypeCriterion);

    conjunction.setAnd(andCriterion);
    disjunction.add(conjunction);
    filter.setOr(disjunction);

    return filter;
  }

  @Nonnull
  private static Criterion buildEntityCriterion(@Nonnull final Urn entityUrn) {
    return CriterionUtils.buildCriterion(
        ENTITY_URN_FIELD_NAME, Condition.EQUAL, entityUrn.toString());
  }

  @Nonnull
  private static Criterion buildEntitiesCriterion(@Nonnull final Set<Urn> entityUrns) {
    final StringArray entityUrnsArray =
        entityUrns.stream().map(Urn::toString).collect(Collectors.toCollection(StringArray::new));
    return CriterionUtils.buildCriterion(ENTITY_URN_FIELD_NAME, Condition.EQUAL, entityUrnsArray);
  }

  @Nonnull
  private static Criterion buildChangeTypeCriterion(@Nonnull final EntityChangeType changeType) {
    return CriterionUtils.buildCriterion(
        ENTITY_CHANGE_TYPES_FIELD_NAME, Condition.EQUAL, changeType.toString());
  }

  @Nonnull
  private static Criterion buildUpstreamCriterion() {
    return CriterionUtils.buildCriterion(
        SUBSCRIPTION_TYPES_FIELD_NAME,
        Condition.EQUAL,
        SubscriptionType.UPSTREAM_ENTITY_CHANGE.toString());
  }

  /**
   * Takes in an MCL and determines whether it is eligible to have notifications generated based on
   * it. Returns true if the MCL is eligible for notifications, false otherwise.
   *
   * <p>Events are eligible for notification generation if the event is NOT coming from an initial
   * ingestion run (first time we're hearing about this entity) OR it is but it's an ingestion
   * related notification. OR if they are assertion run events.
   */
  public static boolean isEligibleForNotificationGeneration(
      @Nonnull final MetadataChangeLog event) {
    return !isFromInitialIngestionRun(event)
        || isIngestionRunResultEvent(event)
        || isAssertionRunEvent(event);
  }

  /**
   * This is an initial ingestion run if there is no previous aspect and this MCL has a run ID not
   * equal to DEFAULT_RUN_ID (coming from ingestion). This isn't perfect as it is possible that we
   * run ingestion and then run ingestion again later and add a new aspect like owner or tags.
   */
  public static boolean isFromInitialIngestionRun(@Nonnull final MetadataChangeLog event) {
    return event.getPreviousAspectValue() == null
        && event.hasSystemMetadata()
        && event.getSystemMetadata().hasRunId()
        && !event.getSystemMetadata().getRunId().equals(DEFAULT_RUN_ID);
  }

  public static List<NotificationRecipient> getUniqueHydratedSubscriberRecipients(
      @Nonnull OperationContext opContext,
      List<NotificationRecipient> recipients,
      EntityNameProvider nameProvider) {
    HashSet<String> existingIds = new HashSet<>();
    return recipients.stream()
        .filter(Objects::nonNull)
        .filter(recipient -> existingIds.add(recipient.getId()))
        .map(
            recipient -> {
              recipient.setOrigin(NotificationRecipientOriginType.SUBSCRIPTION);
              if (recipient.hasActor()) {
                // Hydrate recipient display name, if there is one.
                recipient.setDisplayName(
                    nameProvider.getName(opContext, recipient.getActor()), SetMode.IGNORE_NULL);
              }
              return recipient;
            })
        .collect(Collectors.toList());
  }

  private static boolean isIngestionRunResultEvent(@Nonnull final MetadataChangeLog event) {
    return INGESTION_EXECUTION_REQUEST_ASPECTS.contains(event.getAspectName());
  }

  private static boolean isAssertionRunEvent(@Nonnull final MetadataChangeLog event) {
    return Constants.ASSERTION_RUN_EVENT_ASPECT_NAME.equals(event.getAspectName());
  }

  private NotificationUtils() {}
}
