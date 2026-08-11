package com.linkedin.datahub.graphql.util;

import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Reads what a caller selected on the field currently being resolved, straight from the query AST.
 *
 * <p>Use this instead of {@code environment.getSelectionSet()} when a resolver only needs to know
 * which sub-fields were asked for — typically to skip expensive work nobody reads. {@code
 * getSelectionSet()} normalizes the <b>whole operation</b>, which graphql-java caps at 100k fields
 * ({@code ExecutableNormalizedOperationFactory}); on a large recursive query that cap aborts the
 * entire request rather than degrading the one field. Reading the AST is local to the field and
 * cheap.
 *
 * <p><b>These helpers deliberately over-approximate.</b> They ignore {@code @skip}/{@code @include}
 * and inline-fragment type conditions, so a sub-field present in the document but not actually
 * resolved still counts as selected. Callers must be safe in that direction: treat the result as
 * "everything that might be read", and fall back to the complete, always-correct path when in
 * doubt.
 */
public class SelectionSetUtils {

  private SelectionSetUtils() {}

  /**
   * Names of the immediate sub-fields the caller selected on the field being resolved, resolving
   * fragment spreads and inline fragments.
   */
  @Nonnull
  public static Set<String> selectedSubFieldNames(
      @Nonnull final DataFetchingEnvironment environment) {
    final Set<String> names = new HashSet<>();
    final Map<String, FragmentDefinition> fragments = environment.getFragmentsByName();
    for (Field field : environment.getMergedField().getFields()) {
      collectFieldNames(field.getSelectionSet(), fragments, names);
    }
    return names;
  }

  /**
   * True when the caller selected at least one sub-field and every selected sub-field is in {@code
   * allowed}. An empty selection is treated as ineligible rather than as trivially satisfying the
   * constraint.
   */
  public static boolean selectsOnly(
      @Nonnull final DataFetchingEnvironment environment, @Nonnull final Set<String> allowed) {
    final Set<String> selected = selectedSubFieldNames(environment);
    return !selected.isEmpty() && allowed.containsAll(selected);
  }

  private static void collectFieldNames(
      @Nullable final SelectionSet selectionSet,
      final Map<String, FragmentDefinition> fragments,
      final Set<String> out) {
    if (selectionSet == null) {
      return;
    }
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field) {
        out.add(((Field) selection).getName());
      } else if (selection instanceof InlineFragment) {
        collectFieldNames(((InlineFragment) selection).getSelectionSet(), fragments, out);
      } else if (selection instanceof FragmentSpread) {
        final FragmentDefinition fragment = fragments.get(((FragmentSpread) selection).getName());
        if (fragment != null) {
          collectFieldNames(fragment.getSelectionSet(), fragments, out);
        }
      }
    }
  }
}
