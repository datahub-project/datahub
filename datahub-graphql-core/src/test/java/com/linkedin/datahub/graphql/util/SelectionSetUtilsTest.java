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
import java.util.HashMap;
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
  public void testRepeatedFragmentSpreadsAreExpandedOnce() {
    // Each fragment spreads the next one twice, so a walk that re-expands every occurrence costs
    // 2^DEPTH. The document itself is tiny and cycle-free, so it passes validation and reaches the
    // resolver: without memoisation a request like this pins a thread for minutes. Expanding each
    // fragment at most once keeps the walk linear, so this returns immediately.
    final int depth = 30;
    final Map<String, FragmentDefinition> fragments = new HashMap<>();
    for (int i = 0; i < depth; i++) {
      fragments.put(
          "F" + i,
          fragment(
              "F" + i,
              ImmutableList.of(
                  new FragmentSpread("F" + (i + 1)), new FragmentSpread("F" + (i + 1)))));
    }
    fragments.put("F" + depth, fragment("F" + depth, ImmutableList.of(field("total"))));

    final DataFetchingEnvironment env = env(ImmutableList.of(new FragmentSpread("F0")), fragments);

    assertEquals(SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total"));
    assertTrue(SelectionSetUtils.selectsOnly(env, ImmutableSet.of("total")));
  }

  @Test
  public void testAliasedFieldsUseTheirUnderlyingName() {
    // The response key is irrelevant; what matters is which field the server must resolve. An
    // aliased `searchResults` still means hits are being read, so it must disqualify a count-only
    // caller -- treating the alias as the name would silently serve counts to someone wanting hits.
    final DataFetchingEnvironment aliasedHits =
        env(
            ImmutableList.of(Field.newField("searchResults").alias("count").build()),
            Collections.emptyMap());
    assertEquals(
        SelectionSetUtils.selectedSubFieldNames(aliasedHits), ImmutableSet.of("searchResults"));
    assertFalse(SelectionSetUtils.selectsOnly(aliasedHits, ImmutableSet.of("total")));

    final DataFetchingEnvironment aliasedTotal =
        env(
            ImmutableList.of(Field.newField("total").alias("searchResults").build()),
            Collections.emptyMap());
    assertTrue(SelectionSetUtils.selectsOnly(aliasedTotal, ImmutableSet.of("total")));
  }

  @Test
  public void testNestedFieldSelectionsAreNotCollected() {
    // Only the immediate sub-fields matter. Descending into `searchResults { entity { urn } }`
    // would surface `entity`/`urn` as if the caller had selected them at this level, which would
    // disqualify every caller and defeat the point of the check.
    final Field hitsWithSubSelection =
        Field.newField("searchResults")
            .selectionSet(
                selectionSet(
                    ImmutableList.of(
                        Field.newField("entity")
                            .selectionSet(selectionSet(ImmutableList.of(field("urn"))))
                            .build())))
            .build();
    final DataFetchingEnvironment env =
        env(ImmutableList.of(field("total"), hitsWithSubSelection), Collections.emptyMap());

    assertEquals(
        SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total", "searchResults"));
  }

  @Test
  public void testDisallowedFieldIsFoundThroughFragments() {
    // The early exit has to propagate back up through both recursive branches. If it were dropped,
    // selectsOnly would wrongly report a count-only selection for a caller reading hits.
    final DataFetchingEnvironment throughSpread =
        env(
            ImmutableList.of(field("total"), new FragmentSpread("outer")),
            ImmutableMap.of(
                "outer", fragment("outer", ImmutableList.of(new FragmentSpread("inner"))),
                "inner", fragment("inner", ImmutableList.of(field("searchResults")))));
    assertFalse(SelectionSetUtils.selectsOnly(throughSpread, ImmutableSet.of("total")));

    final DataFetchingEnvironment throughInlineFragment =
        env(
            ImmutableList.of(
                field("total"),
                InlineFragment.newInlineFragment()
                    .selectionSet(selectionSet(ImmutableList.of(field("searchResults"))))
                    .build()),
            Collections.emptyMap());
    assertFalse(SelectionSetUtils.selectsOnly(throughInlineFragment, ImmutableSet.of("total")));
  }

  @Test
  public void testCyclicFragmentsTerminate() {
    // graphql-java rejects fragment cycles before execution, so this should be unreachable in
    // practice -- but expanding each fragment once makes the walk terminate anyway rather than
    // recursing until the stack dies.
    final DataFetchingEnvironment env =
        env(
            ImmutableList.of(new FragmentSpread("a")),
            ImmutableMap.of(
                "a", fragment("a", ImmutableList.of(field("total"), new FragmentSpread("b"))),
                "b", fragment("b", ImmutableList.of(new FragmentSpread("a")))));

    assertEquals(SelectionSetUtils.selectedSubFieldNames(env), ImmutableSet.of("total"));
    assertTrue(SelectionSetUtils.selectsOnly(env, ImmutableSet.of("total")));
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
    // The disallowed field is only in the second merged field, so the loop over merged fields must
    // keep going rather than answering from the first one alone.
    assertFalse(SelectionSetUtils.selectsOnly(env, ImmutableSet.of("total")));
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

    // Same for a directive on the spread itself, which is a separate branch of the walk.
    final DataFetchingEnvironment skippedSpread =
        env(
            ImmutableList.of(
                field("total"),
                FragmentSpread.newFragmentSpread("hits")
                    .directives(Collections.singletonList(skipIfTrue()))
                    .build()),
            ImmutableMap.of("hits", fragment("hits", ImmutableList.of(field("searchResults")))));
    assertFalse(SelectionSetUtils.selectsOnly(skippedSpread, ImmutableSet.of("total")));
  }

  private static Directive skipIfTrue() {
    return Directive.newDirective()
        .name("skip")
        .argument(Argument.newArgument("if", BooleanValue.newBooleanValue(true).build()).build())
        .build();
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
