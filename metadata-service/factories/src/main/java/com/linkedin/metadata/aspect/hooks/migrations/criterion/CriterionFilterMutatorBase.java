package com.linkedin.metadata.aspect.hooks.migrations.criterion;

import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;

import com.linkedin.data.DataList;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.RetrieverContext;
import com.linkedin.metadata.aspect.hooks.AspectMigrationMutator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * v1 → v2 migration for aspects whose payloads embed {@link
 * com.linkedin.metadata.query.filter.Filter} / {@link
 * com.linkedin.metadata.query.filter.Criterion}: strips legacy {@code value} and coalesces into
 * {@code values} ({@link #sanitizeFilterDataMap}, {@link #sanitizeInPlaceOnCopy}). Subclasses
 * supply only {@link #getAspectName()}.
 */
public abstract class CriterionFilterMutatorBase extends AspectMigrationMutator {

  private static final String FIELD_CRITERIA = "criteria";
  private static final String FIELD_OR = "or";
  private static final String FIELD_AND = "and";
  private static final String FIELD_VALUE = "value";
  private static final String FIELD_VALUES = "values";

  /** Aspects that use criterion filter payloads handled by this migration family. */
  public static boolean isSupportedAspect(@Nonnull String aspectName) {
    return DATAHUB_VIEW_INFO_ASPECT_NAME.equals(aspectName)
        || DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME.equals(aspectName);
  }

  /**
   * For every criterion under {@code filter}: if legacy {@code value} is present (including {@code
   * null} or empty string), removes it. When {@code value} was a non-blank string, ensures it
   * appears in {@code values} (appends when missing). After removal, if {@code values} is absent or
   * not a list (optional in stored JSON), sets it to an empty {@link DataList} to match the PDL
   * default {@code values = []}.
   *
   * @return {@code true} if {@code filter} was modified
   */
  public static boolean sanitizeFilterDataMap(@Nullable DataMap filter) {
    if (filter == null) {
      return false;
    }
    boolean changed = false;
    Object criteria = filter.get(FIELD_CRITERIA);
    if (criteria instanceof DataList) {
      changed |= sanitizeCriterionList((DataList) criteria);
    }
    Object or = filter.get(FIELD_OR);
    if (or instanceof DataList) {
      changed |= sanitizeOrList((DataList) or);
    }
    return changed;
  }

  /**
   * Runs criterion {@code value} → {@code values} normalization on {@code copy} (already a deep
   * copy of the aspect payload).
   *
   * @return {@code true} if any criterion map under the aspect's filter was modified
   */
  public static boolean sanitizeInPlaceOnCopy(
      @Nonnull String aspectName, @Nonnull RecordTemplate copy) {
    if (!isSupportedAspect(aspectName)) {
      return false;
    }
    DataMap root = copy.data();
    if (DATAHUB_VIEW_INFO_ASPECT_NAME.equals(aspectName)) {
      DataMap definition = root.getDataMap("definition");
      if (definition == null) {
        return false;
      }
      return sanitizeFilterDataMap(definition.getDataMap("filter"));
    }
    return sanitizeFilterDataMap(root.getDataMap("filter"));
  }

  @Override
  public final long getSourceVersion() {
    return 1L;
  }

  @Override
  public final long getTargetVersion() {
    return 2L;
  }

  /**
   * Test entry point for the same logic as the protected {@link #transform}; only for callers in
   * this package.
   */
  RecordTemplate applyTransformForTest(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    return transform(sourceAspect, context);
  }

  @Override
  @Nullable
  protected final RecordTemplate transform(
      @Nonnull RecordTemplate sourceAspect, @Nonnull RetrieverContext context) {
    final RecordTemplate copy;
    try {
      copy = sourceAspect.copy();
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          "Failed to copy aspect for criterion migration: " + getAspectName(), e);
    }
    if (!sanitizeInPlaceOnCopy(getAspectName(), copy)) {
      return null;
    }
    return copy;
  }

  private static boolean sanitizeOrList(@Nullable DataList orList) {
    if (orList == null) {
      return false;
    }
    boolean changed = false;
    for (Object conjunctObj : orList) {
      if (!(conjunctObj instanceof DataMap)) {
        continue;
      }
      DataMap conjunct = (DataMap) conjunctObj;
      Object and = conjunct.get(FIELD_AND);
      if (and instanceof DataList) {
        changed |= sanitizeCriterionList((DataList) and);
      }
    }
    return changed;
  }

  private static boolean sanitizeCriterionList(@Nullable DataList criteria) {
    if (criteria == null) {
      return false;
    }
    boolean changed = false;
    for (Object criterionObj : criteria) {
      if (!(criterionObj instanceof DataMap)) {
        continue;
      }
      changed |= migrateLegacyCriterionValue((DataMap) criterionObj);
    }
    return changed;
  }

  /**
   * @return {@code true} if the criterion map was modified (including adding an explicit empty
   *     {@code values} list after dropping legacy {@code value})
   */
  static boolean migrateLegacyCriterionValue(DataMap criterion) {
    if (!criterion.containsKey(FIELD_VALUE)) {
      return false;
    }
    Object rawValue = criterion.get(FIELD_VALUE);
    if (rawValue instanceof String && !((String) rawValue).isEmpty()) {
      DataList values = getOrCreateValuesList(criterion);
      if (!listContainsString(values, (String) rawValue)) {
        values.add(rawValue);
      }
    }
    criterion.remove(FIELD_VALUE);
    ensureValuesIsDataList(criterion);
    return true;
  }

  /**
   * {@code values} is optional in serialized maps; normalize to an explicit empty list when missing
   * or wrong-typed so the payload matches {@link com.linkedin.metadata.query.filter.Criterion}.
   */
  private static void ensureValuesIsDataList(DataMap criterion) {
    Object existing = criterion.get(FIELD_VALUES);
    if (!(existing instanceof DataList)) {
      criterion.put(FIELD_VALUES, new DataList());
    }
  }

  private static DataList getOrCreateValuesList(DataMap criterion) {
    Object existing = criterion.get(FIELD_VALUES);
    if (existing instanceof DataList) {
      return (DataList) existing;
    }
    DataList created = new DataList();
    criterion.put(FIELD_VALUES, created);
    return created;
  }

  private static boolean listContainsString(DataList list, String target) {
    for (Object o : list) {
      if (target.equals(o)) {
        return true;
      }
    }
    return false;
  }
}
