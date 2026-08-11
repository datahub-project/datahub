package com.linkedin.datahub.graphql.util;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import graphql.execution.MergedField;
import graphql.language.Argument;
import graphql.language.BooleanValue;
import graphql.language.Directive;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class SelectionSetUtilsTest {

  @Test
  public void testPlainFieldsAreCollected() {
    final DataFetchingEnvironment env =
        env(ImmutableList.of(field("total"), field("__typename")), Collections.emptyMap());

    assertEquals(
        SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total", "__typename"));
  }

  @Test
  public void testFragmentSpreadsAreResolvedRecursively() {
    // The production query reaches most fields through nested fragments, so a spread inside a
    // spread must still surface the leaf field names.
    final Map<String, FragmentDefinition> fragments =
        ImmutableMap.of(
            "outer", fragment("outer", ImmutableList.of(new FragmentSpread("inner"))),
            "inner", fragment("inner", ImmutableList.of(field("searchResults"))));
    final DataFetchingEnvironment env =
        env(ImmutableList.of(field("total"), new FragmentSpread("outer")), fragments);

    assertEquals(
        SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total", "searchResults"));
  }

  @Test
  public void testUnknownFragmentSpreadIsIgnored() {
    final DataFetchingEnvironment env =
        env(
            ImmutableList.of(field("total"), new FragmentSpread("missing")),
            Collections.emptyMap());

    assertEquals(SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total"));
  }

  @Test
  public void testInlineFragmentsAreTraversed() {
    final DataFetchingEnvironment env =
        env(
            ImmutableList.of(
                field("total"),
                InlineFragment.newInlineFragment()
                    .selectionSet(selectionSet(ImmutableList.of(field("searchResults"))))
                    .build()),
            Collections.emptyMap());

    assertEquals(
        SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total", "searchResults"));
  }

  @Test
  public void testAllMergedFieldsAreInspected() {
    // The same field selected twice merges into one MergedField; reading only the first would miss
    // what the second asked for.
    final Field countOnly =
        Field.newField("entities")
            .selectionSet(selectionSet(ImmutableList.of(field("total"))))
            .build();
    final Field withHits =
        Field.newField("entities")
            .selectionSet(selectionSet(ImmutableList.of(field("searchResults"))))
            .build();
    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    Mockito.when(env.getMergedField())
        .thenReturn(MergedField.newMergedField(ImmutableList.of(countOnly, withHits)).build());
    Mockito.when(env.getFragmentsByName()).thenReturn(Collections.emptyMap());

    assertEquals(
        SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total", "searchResults"));
  }

  @Test
  public void testSkipDirectiveIsIgnored() {
    // Documented over-approximation: a sub-field that will be skipped at runtime still counts as
    // selected, so callers fall back to their complete path rather than skipping work wrongly.
    final Field skipped =
        Field.newField("searchResults")
            .directives(
                Collections.singletonList(
                    Directive.newDirective()
                        .name("skip")
                        .argument(
                            Argument.newArgument("if", BooleanValue.newBooleanValue(true).build())
                                .build())
                        .build()))
            .build();
    final DataFetchingEnvironment env =
        env(ImmutableList.of(field("total"), skipped), Collections.emptyMap());

    assertTrue(SelectionSetUtils.selectedSubFieldNames(env).contains("searchResults"));
    assertFalse(SelectionSetUtils.selectsOnly(env, ImmutableSet.of("total")));
  }

  @Test
  public void testSelectsOnly() {
    final DataFetchingEnvironment subset =
        env(ImmutableList.of(field("total")), Collections.emptyMap());
    assertTrue(SelectionSetUtils.selectsOnly(subset, ImmutableSet.of("total", "__typename")));

    final DataFetchingEnvironment superset =
        env(ImmutableList.of(field("total"), field("searchResults")), Collections.emptyMap());
    assertFalse(SelectionSetUtils.selectsOnly(superset, ImmutableSet.of("total", "__typename")));

    // An empty selection is ineligible rather than trivially satisfying the constraint.
    final DataFetchingEnvironment empty = env(Collections.emptyList(), Collections.emptyMap());
    assertFalse(SelectionSetUtils.selectsOnly(empty, ImmutableSet.of("total", "__typename")));
  }

  private static Field field(final String name) {
    return Field.newField(name).build();
  }

  private static SelectionSet selectionSet(final List<? extends Selection<?>> selections) {
    final SelectionSet.Builder builder = SelectionSet.newSelectionSet();
    selections.forEach(builder::selection);
    return builder.build();
  }

  private static FragmentDefinition fragment(
      final String name, final List<? extends Selection<?>> selections) {
    return FragmentDefinition.newFragmentDefinition()
        .name(name)
        .selectionSet(selectionSet(selections))
        .build();
  }

  private static DataFetchingEnvironment env(
      final List<? extends Selection<?>> selections,
      final Map<String, FragmentDefinition> fragments) {
    final Field parent = Field.newField("entities").selectionSet(selectionSet(selections)).build();
    final DataFetchingEnvironment env = mock(DataFetchingEnvironment.class);
    Mockito.when(env.getMergedField()).thenReturn(MergedField.newMergedField(parent).build());
    Mockito.when(env.getFragmentsByName()).thenReturn(fragments);
    return env;
  }
}
