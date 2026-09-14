package com.linkedin.metadata.utils.columnview;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.view.DataHubColumnViewColumn;
import com.linkedin.view.DataHubColumnViewColumnDisplay;
import com.linkedin.view.DataHubColumnViewColumnType;
import com.linkedin.view.DataHubColumnViewInfo;
import com.linkedin.view.DataHubColumnViewTarget;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * The single server-side home for "what kinds of columns exist in a Column View, which are valid
 * for which target, where each kind's data comes from, and which parameters each kind takes".
 *
 * <p>Everything else (validator, resolvers, mappers) asks this class rather than switching on
 * {@link DataHubColumnViewColumnType} directly. Dispatch is always a single lookup on {@code type};
 * the {@code *Params} records are opaque to dispatch. Relationship columns are LEAF enum values:
 * each carries its (edge, direction) here so nothing user-supplied names a graph edge.
 */
public final class ColumnViewColumnKinds {

  /** Where a column kind's values come from. */
  public enum Source {
    /** An aspect of the parent dataset (schemaMetadata, editableSchemaMetadata, profiles). */
    ASPECT,
    /** An aspect of the schemaField entity itself (structuredProperties, globalTags...). */
    FIELD_ENTITY,
    /** A graph edge from the schemaField; fetched separately via the graph service. */
    GRAPH
  }

  /** Per-kind spec. {@code edge}/{@code direction} are set only for {@link Source#GRAPH}. */
  @Value
  public static class Spec {
    DataHubColumnViewColumnType type;
    Source source;
    @Nullable String edge;
    @Nullable RelationshipDirection direction;

    /** INCOMING edges that can fan out to thousands of fields; the UI caps and pages these. */
    boolean highFanout;

    /** At most one related entity by construction (e.g. a field has one logical parent). */
    boolean singleValued;

    public boolean isGraph() {
      return source == Source.GRAPH;
    }
  }

  private static final Map<DataHubColumnViewColumnType, Spec> SPECS =
      new EnumMap<>(DataHubColumnViewColumnType.class);

  static {
    aspect(DataHubColumnViewColumnType.TYPE);
    aspect(DataHubColumnViewColumnType.NATIVE_TYPE);
    aspect(DataHubColumnViewColumnType.LENGTH);
    aspect(DataHubColumnViewColumnType.PRECISION_SCALE);
    aspect(DataHubColumnViewColumnType.NULLABLE);
    aspect(DataHubColumnViewColumnType.PRIMARY_KEY);
    aspect(DataHubColumnViewColumnType.PARTITION_KEY);
    aspect(DataHubColumnViewColumnType.DESCRIPTION);
    aspect(DataHubColumnViewColumnType.TAGS);
    aspect(DataHubColumnViewColumnType.GLOSSARY_TERMS);
    aspect(DataHubColumnViewColumnType.BUSINESS_ATTRIBUTE);
    aspect(DataHubColumnViewColumnType.STATS);
    graph(
        DataHubColumnViewColumnType.LOGICAL_PARENT,
        "PhysicalInstanceOf",
        RelationshipDirection.OUTGOING,
        false,
        true);
    graph(
        DataHubColumnViewColumnType.PHYSICAL_CHILDREN,
        "PhysicalInstanceOf",
        RelationshipDirection.INCOMING,
        true,
        false);
    graph(
        DataHubColumnViewColumnType.UPSTREAM_COLUMNS,
        "DownstreamOf",
        RelationshipDirection.OUTGOING,
        true,
        false);
    graph(
        DataHubColumnViewColumnType.DOWNSTREAM_COLUMNS,
        "DownstreamOf",
        RelationshipDirection.INCOMING,
        true,
        false);
    graph(
        DataHubColumnViewColumnType.FOREIGN_KEY_TO,
        "ForeignKeyTo",
        RelationshipDirection.OUTGOING,
        false,
        false);
    graph(
        DataHubColumnViewColumnType.REFERENCED_BY,
        "ForeignKeyTo",
        RelationshipDirection.INCOMING,
        true,
        false);
    fieldEntity(DataHubColumnViewColumnType.STRUCTURED_PROPERTY);
    fieldEntity(DataHubColumnViewColumnType.LABEL);
  }

  private static void aspect(DataHubColumnViewColumnType t) {
    SPECS.put(t, new Spec(t, Source.ASPECT, null, null, false, false));
  }

  private static void fieldEntity(DataHubColumnViewColumnType t) {
    SPECS.put(t, new Spec(t, Source.FIELD_ENTITY, null, null, false, false));
  }

  private static void graph(
      DataHubColumnViewColumnType t,
      String edge,
      RelationshipDirection direction,
      boolean highFanout,
      boolean singleValued) {
    SPECS.put(t, new Spec(t, Source.GRAPH, edge, direction, highFanout, singleValued));
  }

  /** Column kinds that are valid per target table. */
  private static final Map<DataHubColumnViewTarget, Set<DataHubColumnViewColumnType>>
      ALLOWED_BY_TARGET =
          Map.of(
              DataHubColumnViewTarget.DATASET_SCHEMA_FIELDS,
              Collections.unmodifiableSet(EnumSet.allOf(DataHubColumnViewColumnType.class)));

  /** Column kinds that MUST carry {@code structuredPropertyParams}. */
  private static final Set<DataHubColumnViewColumnType> REQUIRES_STRUCTURED_PROPERTY_PARAMS =
      EnumSet.of(DataHubColumnViewColumnType.STRUCTURED_PROPERTY);

  /** Column kinds that MUST carry {@code labelParams}. */
  private static final Set<DataHubColumnViewColumnType> REQUIRES_LABEL_PARAMS =
      EnumSet.of(DataHubColumnViewColumnType.LABEL);

  /** Entity types a LABEL column may point at. */
  private static final Set<String> LABEL_ENTITY_TYPES =
      Set.of(Constants.TAG_ENTITY_NAME, Constants.GLOSSARY_TERM_ENTITY_NAME);

  /** Maximum GRAPH columns a single view may carry (each is a separate graph query per page). */
  public static final int MAX_GRAPH_COLUMNS = 3;

  /** Bounds for {@code display.maxItems} / {@code display.width}. */
  public static final int MIN_MAX_ITEMS = 1;

  public static final int MAX_MAX_ITEMS = 20;
  public static final int MIN_WIDTH = 40;
  public static final int MAX_WIDTH = 2000;

  /** Size caps keeping one Column View aspect — and the inputs that build it — bounded. */
  public static final int MAX_COLUMNS = 50;

  public static final int MAX_NAME_LENGTH = 200;
  public static final int MAX_DESCRIPTION_LENGTH = 2000;
  public static final int MAX_CUSTOM_ENTRIES = 10;
  public static final int MAX_CUSTOM_KEY_LENGTH = 64;
  public static final int MAX_CUSTOM_VALUE_LENGTH = 512;
  public static final int MAX_FILTER_CRITERIA = 20;
  public static final int MAX_FILTER_VALUES = 50;

  @Nonnull
  public static Spec spec(@Nonnull final DataHubColumnViewColumnType type) {
    return SPECS.get(type);
  }

  public static boolean isGraph(@Nonnull final DataHubColumnViewColumnType type) {
    return spec(type).isGraph();
  }

  public static boolean isGraph(@Nonnull final DataHubColumnViewColumn column) {
    return isGraph(column.getType());
  }

  @Nonnull
  public static Set<DataHubColumnViewColumnType> allowedFor(
      @Nonnull final DataHubColumnViewTarget target) {
    return ALLOWED_BY_TARGET.getOrDefault(target, Collections.emptySet());
  }

  public static boolean isAllowedFor(
      @Nonnull final DataHubColumnViewColumnType type,
      @Nonnull final DataHubColumnViewTarget target) {
    return allowedFor(target).contains(type);
  }

  public static boolean requiresStructuredPropertyParams(
      @Nonnull final DataHubColumnViewColumnType type) {
    return REQUIRES_STRUCTURED_PROPERTY_PARAMS.contains(type);
  }

  public static boolean requiresLabelParams(@Nonnull final DataHubColumnViewColumnType type) {
    return REQUIRES_LABEL_PARAMS.contains(type);
  }

  /** True for urn:li:tag:... and urn:li:glossaryTerm:... */
  public static boolean isLabelUrn(@Nonnull final Urn urn) {
    return LABEL_ENTITY_TYPES.contains(urn.getEntityType());
  }

  /**
   * Returns the list of human-readable problems with the column's parameters (empty when
   * consistent): each params record present iff the kind requires it; LABEL urn must be a tag or
   * glossary term; {@code display} bounds. {@code display.custom} keys and values are free-form
   * (renderer hints the server never interprets) but capped in number and length.
   */
  @Nonnull
  public static List<String> paramProblems(@Nonnull final DataHubColumnViewColumn column) {
    final DataHubColumnViewColumnType type = column.getType();
    final List<String> problems = new ArrayList<>();
    if (column.hasStructuredPropertyParams() != requiresStructuredPropertyParams(type)) {
      problems.add(
          String.format(
              "Column kind %s must %s structuredPropertyParams",
              type, requiresStructuredPropertyParams(type) ? "carry" : "not carry"));
    }
    if (column.hasLabelParams() != requiresLabelParams(type)) {
      problems.add(
          String.format(
              "Column kind %s must %s labelParams",
              type, requiresLabelParams(type) ? "carry" : "not carry"));
    }
    if (column.hasLabelParams() && !isLabelUrn(column.getLabelParams().getUrn())) {
      problems.add(
          String.format(
              "Label urn %s must be a tag or glossary term", column.getLabelParams().getUrn()));
    }
    if (column.hasDisplay()) {
      final DataHubColumnViewColumnDisplay display = column.getDisplay();
      if (display.hasMaxItems()
          && (display.getMaxItems() < MIN_MAX_ITEMS || display.getMaxItems() > MAX_MAX_ITEMS)) {
        problems.add(
            String.format(
                "display.maxItems must be between %d and %d", MIN_MAX_ITEMS, MAX_MAX_ITEMS));
      }
      if (display.hasWidth()
          && (display.getWidth() < MIN_WIDTH || display.getWidth() > MAX_WIDTH)) {
        problems.add(
            String.format("display.width must be between %d and %d", MIN_WIDTH, MAX_WIDTH));
      }
      if (display.hasCustom()) {
        final Map<String, String> custom = display.getCustom();
        if (custom.size() > MAX_CUSTOM_ENTRIES) {
          problems.add(
              String.format("display.custom may hold at most %d entries", MAX_CUSTOM_ENTRIES));
        }
        custom.forEach(
            (key, value) -> {
              if (key.length() > MAX_CUSTOM_KEY_LENGTH
                  || value == null
                  || value.length() > MAX_CUSTOM_VALUE_LENGTH) {
                problems.add(
                    String.format(
                        "display.custom entry '%s' exceeds %d-character key / %d-character value",
                        key, MAX_CUSTOM_KEY_LENGTH, MAX_CUSTOM_VALUE_LENGTH));
              }
            });
      }
    }
    return problems;
  }

  /** Convenience: true when {@link #paramProblems} is empty. */
  public static boolean hasConsistentParams(@Nonnull final DataHubColumnViewColumn column) {
    return paramProblems(column).isEmpty();
  }

  /**
   * Whole-aspect size caps: name/description length, column count, filter criteria and values.
   * Per-column kind/params rules live in {@link #paramProblems}; the validator applies both.
   */
  @Nonnull
  public static List<String> definitionProblems(@Nonnull final DataHubColumnViewInfo info) {
    final List<String> problems = new ArrayList<>();
    if (info.getName().length() > MAX_NAME_LENGTH) {
      problems.add(String.format("name may be at most %d characters", MAX_NAME_LENGTH));
    }
    if (info.hasDescription() && info.getDescription().length() > MAX_DESCRIPTION_LENGTH) {
      problems.add(
          String.format("description may be at most %d characters", MAX_DESCRIPTION_LENGTH));
    }
    final int columns = info.getDefinition().getColumns().size();
    if (columns > MAX_COLUMNS) {
      problems.add(
          String.format(
              "A Column View may hold at most %d columns (found %d)", MAX_COLUMNS, columns));
    }
    if (info.getDefinition().hasFilter()) {
      final Filter filter = info.getDefinition().getFilter();
      int criteria = 0;
      if (filter.hasOr()) {
        for (ConjunctiveCriterion conjunction : filter.getOr()) {
          for (Criterion criterion : conjunction.getAnd()) {
            criteria++;
            if (criterion.getValues().size() > MAX_FILTER_VALUES) {
              problems.add(
                  String.format(
                      "Filter on '%s' may list at most %d values",
                      criterion.getField(), MAX_FILTER_VALUES));
            }
          }
        }
      }
      if (criteria > MAX_FILTER_CRITERIA) {
        problems.add(
            String.format(
                "A Column View filter may hold at most %d criteria (found %d)",
                MAX_FILTER_CRITERIA, criteria));
      }
    }
    return problems;
  }

  /**
   * A stable identity for duplicate detection and sort-column matching. Two columns with equal
   * identity render the same thing. {@code display} is NOT part of identity.
   *
   * <pre>
   *   TYPE
   *   UPSTREAM_COLUMNS
   *   STRUCTURED_PROPERTY:&lt;urn&gt;
   *   LABEL:&lt;urn&gt;
   * </pre>
   */
  @Nonnull
  public static String identity(@Nonnull final DataHubColumnViewColumn column) {
    if (column.hasStructuredPropertyParams()) {
      return identity(column.getType(), column.getStructuredPropertyParams().getUrn().toString());
    }
    if (column.hasLabelParams()) {
      return identity(column.getType(), column.getLabelParams().getUrn().toString());
    }
    return identity(column.getType(), null);
  }

  @Nonnull
  public static String identity(
      @Nonnull final DataHubColumnViewColumnType type, @Nullable final String param) {
    return param == null ? type.name() : type.name() + ":" + param;
  }

  private ColumnViewColumnKinds() {}
}
