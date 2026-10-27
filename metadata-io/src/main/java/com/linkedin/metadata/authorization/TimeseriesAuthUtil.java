package com.linkedin.metadata.authorization;

import static com.linkedin.metadata.Constants.DASHBOARD_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATASET_OPERATION_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_PROFILE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME;
import static com.linkedin.metadata.authorization.ApiGroup.TIMESERIES;
import static com.linkedin.metadata.authorization.ApiOperation.READ;

import com.datahub.authorization.AuthUtil;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;

/**
 * Authorization for timeseries aspect reads. Composes entity READ (via {@link
 * EntityAuthorizationUtils#canViewEntity}, so EDIT/DELETE grant read), optional {@code
 * VIEW_DATASET_*} content privileges, and TIMESERIES READ for dedicated timeseries APIs.
 */
public final class TimeseriesAuthUtil {

  private static final String URN_FIELD = "urn";

  private static final Map<Pair<String, String>, PoliciesConfig.Privilege> SENSITIVE_ASPECTS =
      Map.of(
          Pair.of(DATASET_ENTITY_NAME, DATASET_PROFILE_ASPECT_NAME),
          PoliciesConfig.VIEW_DATASET_PROFILE_PRIVILEGE,
          Pair.of(DATASET_ENTITY_NAME, DATASET_USAGE_STATISTICS_ASPECT_NAME),
          PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE,
          Pair.of(DATASET_ENTITY_NAME, DATASET_OPERATION_ASPECT_NAME),
          PoliciesConfig.VIEW_DATASET_OPERATIONS_PRIVILEGE,
          Pair.of(DASHBOARD_ENTITY_NAME, DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
          PoliciesConfig.VIEW_DATASET_USAGE_PRIVILEGE);

  private TimeseriesAuthUtil() {}

  /** Entity READ using the shared ENTITY READ privilege map (view, get, edit, or delete). */
  public static boolean canReadEntity(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (opContext.isSystemAuth()) {
      return true;
    }
    return EntityAuthorizationUtils.canViewEntity(opContext, urn);
  }

  /**
   * TIMESERIES READ ({@code GET_TIMESERIES_ASPECT_PRIVILEGE} or edit/delete entity). No-op when
   * REST API authorization is disabled.
   */
  public static boolean canReadApi(@Nonnull OperationContext opContext, @Nonnull Urn urn) {
    if (opContext.isSystemAuth()) {
      return true;
    }
    return AuthUtil.isAPIAuthorizedUrns(opContext, TIMESERIES, READ, List.of(urn));
  }

  /**
   * Entity READ and, for mapped sensitive aspects, the corresponding {@code VIEW_DATASET_*}
   * privilege. GraphQL, entity GET, and UsageStats use this.
   */
  public static boolean canViewAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nullable String entityName,
      @Nullable String aspectName) {
    if (opContext.isSystemAuth()) {
      return true;
    }
    if (!matchesEntity(urn, entityName) || StringUtils.isBlank(aspectName)) {
      return false;
    }
    if (!canReadEntity(opContext, urn)) {
      return false;
    }
    Optional<PoliciesConfig.Privilege> extra = sensitivePrivilege(urn.getEntityType(), aspectName);
    if (extra.isEmpty()) {
      return true;
    }
    return AuthUtil.isAuthorized(
        opContext, extra.get(), new EntitySpec(urn.getEntityType(), urn.toString()));
  }

  /**
   * TIMESERIES API access and {@link #canViewAspect}. Rest.li {@code getTimeseriesAspectValues} and
   * OpenAPI timeseries scroll use this.
   */
  public static boolean canReadAspect(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nullable String entityName,
      @Nullable String aspectName) {
    return canReadApi(opContext, urn) && canViewAspect(opContext, urn, entityName, aspectName);
  }

  public static boolean isSensitiveMappedAspect(
      @Nullable String entityName, @Nullable String aspectName) {
    if (StringUtils.isBlank(entityName) || StringUtils.isBlank(aspectName)) {
      return false;
    }
    return SENSITIVE_ASPECTS.containsKey(Pair.of(entityName, aspectName));
  }

  /**
   * URNs from EQUAL (non-negated) {@code urn} criteria on a timeseries filter. Used to scope
   * aggregation API authorization.
   */
  @Nonnull
  public static List<Urn> extractUrnsFromFilter(@Nullable Filter filter) {
    if (filter == null) {
      return List.of();
    }
    List<Urn> urns = new ArrayList<>();
    if (filter.hasOr()) {
      for (ConjunctiveCriterion andGroup : filter.getOr()) {
        collectUrnsFromCriteria(andGroup.getAnd(), urns);
      }
    } else if (filter.hasCriteria()) {
      collectUrnsFromCriteria(filter.getCriteria(), urns);
    }
    return List.copyOf(urns);
  }

  public static boolean canReadAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Collection<Urn> urns,
      @Nonnull String entityName,
      @Nonnull String aspectName) {
    return urns.stream().allMatch(urn -> canReadAspect(opContext, urn, entityName, aspectName));
  }

  /**
   * Rest.li {@code getTimeseriesStats}. Mapped sensitive aspects require every OR/AND disjunct to
   * contain a positive {@code urn} EQUAL clause; authorization is then per extracted URN. Unscoped
   * or partially scoped sensitive aggregations are denied. Unmapped aspects use TIMESERIES READ by
   * entity type unless the filter is fully URN-scoped, in which case each URN is checked with
   * {@link #canReadAspect}.
   */
  public static boolean canReadAggregatedStats(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String aspectName,
      @Nullable Filter filter) {
    if (opContext.isSystemAuth()) {
      return true;
    }
    boolean fullyUrnScoped = isFilterFullyUrnScoped(filter);
    if (isSensitiveMappedAspect(entityName, aspectName) && !fullyUrnScoped) {
      return false;
    }
    if (fullyUrnScoped) {
      return canReadAspects(opContext, extractUrnsFromFilter(filter), entityName, aspectName);
    }
    return AuthUtil.isAPIAuthorizedEntityType(opContext, TIMESERIES, READ, entityName);
  }

  /**
   * True when every conjunctive group in the filter has a non-negated {@code urn} EQUAL criterion
   * with at least one value. Mixed OR branches (one URN-scoped, one not) return false.
   */
  public static boolean isFilterFullyUrnScoped(@Nullable Filter filter) {
    if (filter == null) {
      return false;
    }
    if (filter.hasOr()) {
      if (filter.getOr().isEmpty()) {
        return false;
      }
      for (ConjunctiveCriterion andGroup : filter.getOr()) {
        if (!hasPositiveUrnEqual(andGroup != null ? andGroup.getAnd() : null)) {
          return false;
        }
      }
      return true;
    }
    return hasPositiveUrnEqual(filter.hasCriteria() ? filter.getCriteria() : null);
  }

  /**
   * Drop timeseries aspects the actor cannot view. Keys are aspect names on a single entity URN.
   * Used when assembling OpenAPI entity documents.
   */
  @Nonnull
  public static <V> Map<String, V> omitUnauthorizedTimeseriesAspects(
      @Nonnull OperationContext opContext,
      @Nonnull Urn urn,
      @Nonnull Map<String, V> aspectValues,
      @Nonnull Function<String, Boolean> isTimeseries) {
    if (aspectValues.isEmpty()) {
      return aspectValues;
    }
    Map<String, V> allowed = new LinkedHashMap<>();
    for (Map.Entry<String, V> entry : aspectValues.entrySet()) {
      if (Boolean.TRUE.equals(isTimeseries.apply(entry.getKey()))
          && !canViewAspect(opContext, urn, urn.getEntityType(), entry.getKey())) {
        continue;
      }
      allowed.put(entry.getKey(), entry.getValue());
    }
    return allowed;
  }

  private static Optional<PoliciesConfig.Privilege> sensitivePrivilege(
      @Nonnull String entityName, @Nonnull String aspectName) {
    return Optional.ofNullable(SENSITIVE_ASPECTS.get(Pair.of(entityName, aspectName)));
  }

  private static boolean matchesEntity(@Nonnull Urn urn, @Nullable String entityName) {
    return StringUtils.isNotBlank(entityName) && entityName.equals(urn.getEntityType());
  }

  private static boolean hasPositiveUrnEqual(@Nullable Iterable<Criterion> criteria) {
    if (criteria == null) {
      return false;
    }
    for (Criterion criterion : criteria) {
      if (criterion.isNegated()
          || !URN_FIELD.equals(criterion.getField())
          || (criterion.hasCondition()
              && criterion.getCondition() != com.linkedin.metadata.query.filter.Condition.EQUAL)) {
        continue;
      }
      List<String> values =
          criterion.getValues() != null ? criterion.getValues() : Collections.emptyList();
      for (String value : values) {
        if (StringUtils.isNotBlank(value)) {
          return true;
        }
      }
    }
    return false;
  }

  private static void collectUrnsFromCriteria(
      @Nullable Iterable<Criterion> criteria, @Nonnull List<Urn> urns) {
    if (criteria == null) {
      return;
    }
    for (Criterion criterion : criteria) {
      if (criterion.isNegated()
          || !URN_FIELD.equals(criterion.getField())
          || (criterion.hasCondition()
              && criterion.getCondition() != com.linkedin.metadata.query.filter.Condition.EQUAL)) {
        continue;
      }
      List<String> values =
          criterion.getValues() != null ? criterion.getValues() : Collections.emptyList();
      for (String value : values) {
        if (StringUtils.isNotBlank(value)) {
          urns.add(UrnUtils.getUrn(value));
        }
      }
    }
  }
}
