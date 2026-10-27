package com.linkedin.metadata.search;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.utils.SchemaFieldUtils;
import java.util.Set;
import java.util.function.BiFunction;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

/**
 * What the lightning lineage search reads off a urn: its platform and environment facets, the
 * parent of a schema field, the dataset a schema field is grouped under, and whether it passes the
 * lightning filters.
 *
 * <p>Shared by {@link LineageSearchService} and the in-memory lineage graph server, which answers
 * the same lightning search next to the graph; both must compute exactly the same values.
 */
@Slf4j
public final class LightningUrnFields {

  static final String PARENT_FILTER = "parent";

  /** Filter fields the graph-only path can answer from a urn, without the entity index. */
  public static final Set<String> FILTER_FIELDS = Set.of("platform", "origin", PARENT_FILTER);

  private static final Set<String> PLATFORM_ENTITY_TYPES =
      ImmutableSet.of(
          DATASET_ENTITY_NAME,
          CHART_ENTITY_NAME,
          DASHBOARD_ENTITY_NAME,
          DATA_FLOW_ENTITY_NAME,
          DATA_JOB_ENTITY_NAME);

  private LightningUrnFields() {}

  @Nullable
  public static String platform(String entityType, Urn entityUrn) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityType)) {
      return fromParent(entityUrn, LightningUrnFields::platform);
    }
    String platform = null;
    if (PLATFORM_ENTITY_TYPES.contains(entityType)) {
      if (DATA_JOB_ENTITY_NAME.equals(entityType)) {
        platform = UrnUtils.getUrn(entityUrn.getEntityKey().get(0)).getEntityKey().get(0);
      } else {
        platform = entityUrn.getEntityKey().get(0);
      }
    }
    if ((platform != null) && (!platform.startsWith("urn:li:dataPlatform"))) {
      platform = "urn:li:dataPlatform:" + platform;
    }

    return platform;
  }

  @Nullable
  public static String environment(String entityType, Urn entityUrn) {
    if (SCHEMA_FIELD_ENTITY_NAME.equals(entityType)) {
      return fromParent(entityUrn, LightningUrnFields::environment);
    }
    return DATASET_ENTITY_NAME.equals(entityType) ? entityUrn.getEntityKey().get(2) : null;
  }

  /** The parent of a schema field, which the {@code parent} filter compares against. */
  @Nullable
  public static String parent(Urn urn) {
    return SchemaFieldUtils.parseSchemaFieldUrn(urn)
        .map(parsed -> parsed.getFirst().toString())
        .orElse(null);
  }

  /**
   * A schema field carries neither a platform nor an environment of its own; it takes its parent's,
   * which is nested inside its own urn.
   */
  @Nullable
  private static String fromParent(Urn schemaFieldUrn, BiFunction<String, Urn, String> ofParent) {
    return SchemaFieldUtils.parseSchemaFieldUrn(schemaFieldUrn)
        .map(parsed -> ofParent.apply(parsed.getFirst().getEntityType(), parsed.getFirst()))
        .orElse(null);
  }

  /** The urn a schema field relationship is reported as when schema fields group into datasets. */
  public static Urn schemaFieldReferenceUrn(Urn urn) {
    if (urn.getEntityType().equals(SCHEMA_FIELD_ENTITY_NAME)) {
      try {
        // Get the dataset urn referenced inside the schemaField urn
        return Urn.createFromString(urn.getId());
      } catch (Exception e) {
        log.error("Invalid destination urn: {}", urn.getId(), e);
      }
    }
    return urn;
  }

  /**
   * Whether a urn passes the filters, which the graph-only path answers from the urn alone. A
   * Filter is a disjunction of conjunctions, so the urn passes if every criterion of any one
   * or-branch passes -- evaluating each branch separately rather than pooling their criteria, which
   * would reject urns that satisfy one branch but not another.
   *
   * <p>Negation is honored, so excluding the columns of a node the graph draws folded into another
   * is expressible.
   *
   * @param parent the urn's {@link #parent}, null when it is not a schema field
   */
  public static boolean passes(
      @Nullable String parent,
      @Nullable String platform,
      @Nullable String environment,
      @Nullable Filter filters) {
    if (filters == null || CollectionUtils.isEmpty(filters.getOr())) {
      return true;
    }
    return filters.getOr().stream()
        .anyMatch(
            branch ->
                !branch.hasAnd()
                    || branch.getAnd().stream()
                        .allMatch(
                            criterion ->
                                passesCriterion(parent, platform, environment, criterion)));
  }

  private static boolean passesCriterion(
      @Nullable String parent,
      @Nullable String platform,
      @Nullable String environment,
      Criterion criterion) {
    if (CollectionUtils.isEmpty(criterion.getValues())) {
      return true;
    }
    // Fields outside these are rejected by canDoLightning, which never lets them reach here
    final String value;
    switch (criterion.getField()) {
      case "platform":
        value = platform;
        break;
      case "origin":
        value = environment;
        break;
      case PARENT_FILTER:
        value = parent;
        break;
      default:
        value = null;
        break;
    }

    boolean matches =
        value != null
            && criterion.getValues().stream()
                .anyMatch(
                    candidate ->
                        Condition.CONTAIN.equals(criterion.getCondition())
                            ? value.contains(candidate)
                            : value.equals(candidate));
    return matches != Boolean.TRUE.equals(criterion.isNegated());
  }
}
