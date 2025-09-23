package com.linkedin.metadata.utils;

import static com.linkedin.metadata.Constants.URN_LI_PREFIX;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CriterionUtils {
  private CriterionUtils() {}

  /**
   * This function is meant to validate and correct Filter input for rest.li endpoints.
   *
   * @param inputFilter the rest.li filter parameter
   * @return validated and corrected filter
   */
  @Nullable
  public static Filter validateAndConvert(@Nullable Filter inputFilter) {
    if (inputFilter != null) {
      List<Criterion> invalidCriterion = new ArrayList<>();
      if (inputFilter.hasCriteria()) {
        invalidCriterion.addAll(
            inputFilter.getCriteria().stream()
                .filter(
                    criterion ->
                        (criterion.hasValue() && !criterion.getValue().isEmpty())
                            || !criterion.hasValue())
                .collect(Collectors.toList()));
      }
      if (inputFilter.hasOr()) {
        invalidCriterion.addAll(
            inputFilter.getOr().stream()
                .flatMap(c -> c.getAnd().stream())
                .filter(
                    criterion ->
                        (criterion.hasValue() && !criterion.getValue().isEmpty())
                            || !criterion.hasValue())
                .collect(Collectors.toList()));
      }

      for (Criterion criterion : invalidCriterion) {
        if (criterion.hasValue()) {
          if ((criterion.getValue().contains(",")
                  && !criterion.getValue().startsWith(URN_LI_PREFIX))
              || criterion.getValue().contains(")," + URN_LI_PREFIX)) {
            throw new IllegalArgumentException(
                "Criterion `value` is deprecated and contains an ambiguous comma. Please use `values`.");
          }
          if (criterion.hasValues() && !criterion.getValue().equals(criterion.getValues().get(0))) {
            throw new IllegalArgumentException(
                "Criterion `value` is deprecated and `values`[0] is populated with a conflicting value.");
          }
          // auto-convert
          if (!criterion.hasValues()) {
            log.error(
                "Deprecated use of a filter using Criterion's `value` has been detected and corrected. Please migrate to `values` instead.");
            criterion.setValues(new StringArray(criterion.getValue()));
          }
        }
        // must be set per required field
        criterion.setValue("");
      }
    }
    return inputFilter;
  }

  public static Criterion buildExistsCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.EXISTS, false, Collections.emptyList());
  }

  public static Criterion buildNotExistsCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.EXISTS, true, Collections.emptyList());
  }

  public static Criterion buildIsNullCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.IS_NULL, false, Collections.emptyList());
  }

  public static Criterion buildIsNotNullCriterion(@Nonnull String field) {
    return buildCriterion(field, Condition.IS_NULL, true, Collections.emptyList());
  }

  public static Criterion buildCriterion(
      @Nonnull String field, @Nonnull Condition condition, String... values) {
    return buildCriterion(
        field,
        condition,
        null,
        values == null
            ? Collections.emptyList()
            : Arrays.stream(values).collect(Collectors.toList()));
  }

  public static Criterion buildCriterion(
      @Nonnull String field, @Nonnull Condition condition, Collection<String> values) {
    return buildCriterion(field, condition, false, values);
  }

  public static Criterion buildCriterion(
      @Nonnull String field, @Nonnull Condition condition, boolean negated, String... values) {
    return buildCriterion(
        field,
        condition,
        negated,
        values == null
            ? Collections.emptyList()
            : Arrays.stream(values).collect(Collectors.toList()));
  }

  public static Criterion buildCriterion(
      @Nonnull String field,
      @Nonnull Condition condition,
      Boolean negated,
      Collection<String> values) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(condition);
    criterion.setNegated(negated != null ? negated : false);
    criterion.setValues(values != null ? new StringArray(values) : new StringArray());
    criterion.setValue(""); // deprecated
    return criterion;
  }

  public static ConjunctiveCriterion buildConjunctiveCriterion(Criterion... criteria) {
    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(
        new CriterionArray(Arrays.stream(criteria).collect(Collectors.toList())));
    return conjunctiveCriterion;
  }
}
