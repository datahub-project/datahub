package com.linkedin.metadata.columnview.hooks;

import static com.linkedin.metadata.Constants.DATAHUB_COLUMN_VIEW_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;

import com.datahub.context.OperationFingerprint;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.batch.ChangeMCP;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.aspect.batch.MCPItem;
import com.linkedin.metadata.aspect.plugins.config.AspectPluginConfig;
import com.linkedin.metadata.aspect.plugins.hooks.MCPSideEffect;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.view.DataHubColumnViewColumn;
import com.linkedin.view.DataHubColumnViewColumnArray;
import com.linkedin.view.DataHubColumnViewDefinition;
import com.linkedin.view.DataHubColumnViewInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;
import lombok.extern.slf4j.Slf4j;

/**
 * When a structured property, tag or glossary term is hard-deleted, scrub every {@code
 * dataHubColumnView} that references it (found through the {@code structuredPropertyUrns} / {@code
 * labelUrns} searchable fields) and emit an UPSERT of {@code dataHubColumnViewInfo} with:
 *
 * <ul>
 *   <li>the referencing {@code columns[]} removed
 *   <li>{@code sort} cleared when it pointed at a removed column
 *   <li>filter criteria on {@code structuredProperties.<urn>} removed, and the urn dropped from
 *       {@code tags} / {@code glossaryTerms} criteria values (criterion dropped when empty)
 * </ul>
 *
 * The view itself is never deleted, even when {@code columns} becomes empty. Precedent: {@link
 * com.linkedin.metadata.structuredproperties.hooks.PropertyDefinitionDeleteSideEffect}. Hard
 * deletes surface as a key-aspect DELETE MCL, which is what this side effect is registered on.
 */
@Slf4j
@Getter
@Setter
@Accessors(chain = true)
public class ColumnViewReferenceDeleteSideEffect extends MCPSideEffect {
  public static final Integer SEARCH_SCROLL_SIZE = 1000;

  /**
   * Mirrors the client-side filter field prefix (columnKinds.STRUCTURED_PROPERTY_FILTER_PREFIX).
   */
  public static final String STRUCTURED_PROPERTY_FILTER_PREFIX = "structuredProperties.";

  /** Filter fields whose values are label urns (columnKinds.LABEL_FILTER_FIELDS). */
  public static final Set<String> LABEL_FILTER_FIELDS = Set.of("tags", "glossaryTerms");

  private static final String STRUCTURED_PROPERTY_URNS_FIELD = "structuredPropertyUrns";
  private static final String LABEL_URNS_FIELD = "labelUrns";

  @Nonnull private AspectPluginConfig config;

  @Override
  protected Stream<ChangeMCP> applyMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<ChangeMCP> changeMCPS,
      @Nonnull RetrieverContext retrieverContext) {
    return Stream.of();
  }

  @Override
  protected Stream<MCPItem> postMCPSideEffect(
      @Nonnull OperationFingerprint operationContext,
      Collection<MCLItem> mclItems,
      @Nonnull RetrieverContext retrieverContext) {
    return mclItems.stream()
        .flatMap(item -> scrubReferencingViews(operationContext, item, retrieverContext));
  }

  private Stream<MCPItem> scrubReferencingViews(
      @Nonnull OperationFingerprint operationFingerprint,
      @Nonnull MCLItem mclItem,
      @Nonnull RetrieverContext retrieverContext) {
    final Urn deleted = mclItem.getUrn();
    final AuditStamp auditStamp =
        mclItem.getAuditStamp() != null
            ? mclItem.getAuditStamp()
            : AuditStampUtils.createDefaultAuditStamp();

    List<MCPItem> upserts = new ArrayList<>();
    for (Urn viewUrn : findReferencingViews(deleted, retrieverContext)) {
      Aspect aspect =
          retrieverContext
              .getAspectRetriever()
              .getLatestAspectObject(
                  operationFingerprint, viewUrn, DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME);
      if (aspect == null) {
        continue;
      }
      DataHubColumnViewInfo info;
      try {
        info = new DataHubColumnViewInfo(aspect.data()).copy();
      } catch (CloneNotSupportedException e) {
        log.warn(
            "Could not copy {} for {}; skipping scrub",
            DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME,
            viewUrn);
        continue;
      }
      if (!scrub(info, deleted)) {
        // Search hit but no reference in the stored aspect (stale index); nothing to write.
        continue;
      }
      info.setLastModified(auditStamp);
      upserts.add(
          ChangeItemImpl.builder()
              .urn(viewUrn)
              .aspectName(DATAHUB_COLUMN_VIEW_INFO_ASPECT_NAME)
              .recordTemplate(info)
              .auditStamp(auditStamp)
              .build(retrieverContext.getAspectRetriever()));
    }
    if (!upserts.isEmpty()) {
      log.info("Removed references to deleted {} from {} column view(s)", deleted, upserts.size());
    }
    return upserts.stream();
  }

  /** Column views whose index row lists the urn under structuredPropertyUrns or labelUrns. */
  private static List<Urn> findReferencingViews(
      @Nonnull Urn deleted, @Nonnull RetrieverContext retrieverContext) {
    final Filter filter = referencesFilter(deleted);
    final List<Urn> urns = new ArrayList<>();
    String scrollId = null;
    do {
      ScrollResult result =
          retrieverContext
              .getSearchRetriever()
              .scroll(
                  List.of(DATAHUB_COLUMN_VIEW_ENTITY_NAME), filter, scrollId, SEARCH_SCROLL_SIZE);
      if (result == null) {
        break;
      }
      for (SearchEntity entity : result.getEntities()) {
        urns.add(entity.getEntity());
      }
      scrollId = result.getScrollId();
    } while (scrollId != null);
    return urns;
  }

  private static Filter referencesFilter(@Nonnull Urn deleted) {
    final String value = deleted.toString();
    return new Filter()
        .setOr(
            new ConjunctiveCriterionArray(
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            buildCriterion(
                                STRUCTURED_PROPERTY_URNS_FIELD, Condition.EQUAL, value))),
                new ConjunctiveCriterion()
                    .setAnd(
                        new CriterionArray(
                            buildCriterion(LABEL_URNS_FIELD, Condition.EQUAL, value)))));
  }

  /**
   * Removes every reference to {@code deleted} from the view definition in place. Returns whether
   * anything changed. Never touches the view's identity; an empty {@code columns} list is allowed.
   */
  static boolean scrub(@Nonnull DataHubColumnViewInfo info, @Nonnull Urn deleted) {
    final DataHubColumnViewDefinition definition = info.getDefinition();
    boolean changed = false;

    final DataHubColumnViewColumnArray kept = new DataHubColumnViewColumnArray();
    for (DataHubColumnViewColumn column : definition.getColumns()) {
      if (references(column, deleted)) {
        changed = true;
      } else {
        kept.add(column);
      }
    }
    if (changed) {
      definition.setColumns(kept);
    }

    if (definition.hasSort() && references(definition.getSort().getColumn(), deleted)) {
      definition.removeSort();
      changed = true;
    }

    if (definition.hasFilter()) {
      changed |= scrubFilter(definition, deleted);
    }
    return changed;
  }

  private static boolean references(@Nonnull DataHubColumnViewColumn column, @Nonnull Urn urn) {
    return (column.hasStructuredPropertyParams()
            && urn.equals(column.getStructuredPropertyParams().getUrn()))
        || (column.hasLabelParams() && urn.equals(column.getLabelParams().getUrn()));
  }

  /**
   * Drops {@code structuredProperties.<urn>} criteria and removes the urn from label criteria
   * values; empty criteria, conjunctions and finally the filter itself are dropped.
   */
  private static boolean scrubFilter(
      @Nonnull DataHubColumnViewDefinition definition, @Nonnull Urn deleted) {
    final Filter filter = definition.getFilter();
    if (!filter.hasOr()) {
      return false;
    }
    final String urn = deleted.toString();
    final String propertyField = STRUCTURED_PROPERTY_FILTER_PREFIX + urn;
    boolean changed = false;

    final ConjunctiveCriterionArray keptOr = new ConjunctiveCriterionArray();
    for (ConjunctiveCriterion conjunction : filter.getOr()) {
      final CriterionArray keptAnd = new CriterionArray();
      for (Criterion criterion : conjunction.getAnd()) {
        if (propertyField.equals(criterion.getField())) {
          changed = true;
          continue;
        }
        if (LABEL_FILTER_FIELDS.contains(criterion.getField())
            && criterion.hasValues()
            && criterion.getValues().contains(urn)) {
          changed = true;
          final StringArray values =
              new StringArray(
                  criterion.getValues().stream()
                      .filter(v -> !urn.equals(v))
                      .collect(Collectors.toList()));
          if (values.isEmpty()) {
            continue;
          }
          criterion.setValues(values);
        }
        keptAnd.add(criterion);
      }
      if (!keptAnd.isEmpty()) {
        conjunction.setAnd(keptAnd);
        keptOr.add(conjunction);
      }
    }
    if (!changed) {
      return false;
    }
    if (keptOr.isEmpty()) {
      definition.removeFilter();
    } else {
      filter.setOr(keptOr);
    }
    return true;
  }
}
