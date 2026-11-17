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
    walk(environment, names, null);
    return names;
  }

  /**
   * True when the caller selected at least one sub-field and every selected sub-field is in {@code
   * allowed}. An empty selection is treated as ineligible rather than as trivially satisfying the
   * constraint.
   *
   * <p>Stops at the first sub-field outside {@code allowed} rather than collecting the whole
   * selection, since the answer cannot change after that.
   */
  public static boolean selectsOnly(
      @Nonnull final DataFetchingEnvironment environment, @Nonnull final Set<String> allowed) {
    final Set<String> selected = new HashSet<>();
    final boolean foundDisallowed = walk(environment, selected, allowed);
    return !foundDisallowed && !selected.isEmpty();
  }

  /**
   * @param allowed when non-null, the walk stops as soon as a field outside this set is selected
   * @return true if the walk stopped early on a disallowed field
   */
  private static boolean walk(
      final DataFetchingEnvironment environment,
      final Set<String> out,
      @Nullable final Set<String> allowed) {
    final Map<String, FragmentDefinition> fragments = environment.getFragmentsByName();
    // A fragment reached twice contributes nothing new, and re-expanding it makes a document whose
    // fragments spread each other repeatedly cost 2^depth to walk. Expanding each at most once
    // keeps this linear in the size of the document (and makes cycles harmless).
    final Set<String> visitedFragments = new HashSet<>();
    for (Field field : environment.getMergedField().getFields()) {
      if (collectFieldNames(field.getSelectionSet(), fragments, visitedFragments, out, allowed)) {
        return true;
      }
    }
    return false;
  }

  private static boolean collectFieldNames(
      @Nullable final SelectionSet selectionSet,
      final Map<String, FragmentDefinition> fragments,
      final Set<String> visitedFragments,
      final Set<String> out,
      @Nullable final Set<String> allowed) {
    if (selectionSet == null) {
      return false;
    }
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field) {
        final String name = ((Field) selection).getName();
        out.add(name);
        if (allowed != null && !allowed.contains(name)) {
          return true;
        }
      } else if (selection instanceof InlineFragment) {
        if (collectFieldNames(
            ((InlineFragment) selection).getSelectionSet(),
            fragments,
            visitedFragments,
            out,
            allowed)) {
          return true;
        }
      } else if (selection instanceof FragmentSpread) {
        final String name = ((FragmentSpread) selection).getName();
        final FragmentDefinition fragment = fragments.get(name);
        if (fragment != null
            && visitedFragments.add(name)
            && collectFieldNames(
                fragment.getSelectionSet(), fragments, visitedFragments, out, allowed)) {
          return true;
        }
      }
    }
    return false;
  }
}
