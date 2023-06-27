package com.linkedin.metadata.kafka.hook.notification;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.pegasus2avro.subscription.SubscriptionType;
import com.linkedin.subscription.EntityChangeType;
import com.linkedin.subscription.SubscriptionInfo;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

import static com.linkedin.metadata.AcrylConstants.*;


public class NotificationUtils {

  /**
   * Given an Entity Urn, generates a relative path from it (for rendering in the UI)
   */
  @Nonnull
  public static String generateEntityPath(final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return String.format("/dataset/%s", entityUrn);
      case Constants.CHART_ENTITY_NAME:
        return String.format("/chart/%s", entityUrn);
      case Constants.DASHBOARD_ENTITY_NAME:
        return String.format("/dashboard/%s", entityUrn);
      case Constants.DATA_FLOW_ENTITY_NAME:
        return String.format("/pipeline/%s", entityUrn);
      case Constants.DATA_JOB_ENTITY_NAME:
        return String.format("/task/%s", entityUrn);
      case Constants.TAG_ENTITY_NAME:
        return String.format("/tag/%s", entityUrn);
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return String.format("/glossaryTerm/%s", entityUrn);
      case Constants.DOMAIN_ENTITY_NAME:
        return String.format("/domain/%s", entityUrn);
      case Constants.CONTAINER_ENTITY_NAME:
        return String.format("/container/%s", entityUrn);
      case Constants.CORP_USER_ENTITY_NAME:
        return String.format("/user/%s", entityUrn);
      case Constants.CORP_GROUP_ENTITY_NAME:
        return String.format("/group/%s", entityUrn);
      default:
        return "";
    }
  }

  /**
   * Given an Entity Urn, get the type of the entity
   */
  @Nonnull
  public static String getEntityType(final Urn entityUrn) {
    switch (entityUrn.getEntityType()) {
      case Constants.DATASET_ENTITY_NAME:
        return "Dataset";
      case Constants.CHART_ENTITY_NAME:
        return "Chart";
      case Constants.DASHBOARD_ENTITY_NAME:
        return "Dashboard";
      case Constants.DATA_FLOW_ENTITY_NAME:
        return "Data Pipeline (Flow)";
      case Constants.DATA_JOB_ENTITY_NAME:
        return "Data Task (Job)";
      case Constants.TAG_ENTITY_NAME:
        return "Tag";
      case Constants.GLOSSARY_TERM_ENTITY_NAME:
        return "Glossary Term";
      case Constants.DOMAIN_ENTITY_NAME:
        return "Domain";
      case Constants.CONTAINER_ENTITY_NAME:
        return "Container";
      case Constants.CORP_USER_ENTITY_NAME:
        return "User";
      case Constants.CORP_GROUP_ENTITY_NAME:
        return "Group";
      case Constants.INCIDENT_ENTITY_NAME:
        return "Incident";
      default:
        return "";
    }
  }

  @Nonnull
  public static SubscriptionInfo mapSubscriptionInfo(@Nonnull final EntityResponse entityResponse) {
    return new SubscriptionInfo(entityResponse.getAspects().get(SUBSCRIPTION_INFO_ASPECT_NAME).getValue().data());
  }

  @Nonnull
  public static Filter createSubscriberFilter(@Nonnull final Urn entityUrn, @Nonnull final EntityChangeType changeType) {
    final Criterion entityCriterion = buildEntityCriterion(entityUrn);
    final Criterion changeTypeCriterion = buildChangeTypeCriterion(changeType);
    final CriterionArray criterionArray = new CriterionArray(ImmutableList.of(entityCriterion, changeTypeCriterion));

    return new Filter().setOr(new ConjunctiveCriterionArray(new ConjunctiveCriterion().setAnd(criterionArray)));
  }

  @Nonnull
  public static Filter createDownstreamSubscriberFilter(@Nonnull final Set<Urn> entityUrns,
      @Nonnull final EntityChangeType changeType) {
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
    final Criterion entityCriterion = new Criterion();
    entityCriterion.setField(ENTITY_URN_FIELD_NAME);
    entityCriterion.setValue(entityUrn.toString());
    entityCriterion.setCondition(Condition.EQUAL);

    return entityCriterion;
  }

  @Nonnull
  private static Criterion buildEntitiesCriterion(@Nonnull final Set<Urn> entityUrns) {
    final StringArray entityUrnsArray =
        entityUrns.stream().map(Urn::toString).collect(Collectors.toCollection(StringArray::new));
    final Criterion entityCriterion = new Criterion();
    entityCriterion.setField(ENTITY_URN_FIELD_NAME);
    entityCriterion.setValues(entityUrnsArray);
    entityCriterion.setCondition(Condition.EQUAL);

    return entityCriterion;
  }

  @Nonnull
  private static Criterion buildChangeTypeCriterion(@Nonnull final EntityChangeType changeType) {
    final Criterion changeTypeCriterion = new Criterion();
    changeTypeCriterion.setField(ENTITY_CHANGE_TYPES_FIELD_NAME);
    changeTypeCriterion.setValue(changeType.toString());
    changeTypeCriterion.setValues(new StringArray(changeType.toString()));
    changeTypeCriterion.setCondition(Condition.EQUAL);

    return changeTypeCriterion;
  }

  @Nonnull
  private static Criterion buildUpstreamCriterion() {
    final Criterion entityCriterion = new Criterion();
    entityCriterion.setField(SUBSCRIPTION_TYPES_FIELD_NAME);
    entityCriterion.setValues(new StringArray(SubscriptionType.UPSTREAM_ENTITY_CHANGE.toString()));
    entityCriterion.setValue(SubscriptionType.UPSTREAM_ENTITY_CHANGE.toString());
    entityCriterion.setCondition(Condition.EQUAL);

    return entityCriterion;
  }

  private NotificationUtils() { }

}
