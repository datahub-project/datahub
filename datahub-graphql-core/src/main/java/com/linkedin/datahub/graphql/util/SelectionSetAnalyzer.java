package com.linkedin.datahub.graphql.util;

import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.language.TypeName;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLInterfaceType;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLUnionType;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Collects the field names selected directly on a resolved entity, reading the query AST rather
 * than {@link graphql.schema.DataFetchingFieldSelectionSet}.
 *
 * <p>{@code DataFetchingFieldSelectionSet#getFields()} (and {@code getImmediateFields()}, which
 * shares the same lazy computation) materializes a {@code SelectedField} plus a type-qualified name
 * string for every field at every depth beneath the current one. Entity resolvers run once per
 * resolved entity, so a list of N search results would rebuild that whole subtree N times. For
 * DataHub's deeply nested queries that is enough to exhaust the GMS heap.
 *
 * <p>Only immediate fields matter here: {@link com.linkedin.datahub.graphql.AspectMappingRegistry}
 * maps {@code Type.field} to aspects, and fields nested deeper belong to other types (a nested
 * entity of the same type is resolved by its own resolver, which contributes its own selection to
 * the request-scoped union). Fragments are traversed because they do not introduce a level.
 */
public class SelectionSetAnalyzer {

  private SelectionSetAnalyzer() {}

  /**
   * Returns the names of the fields selected directly on {@code objectTypeName}, resolving inline
   * fragments and fragment spreads whose type condition applies to that type.
   */
  @Nonnull
  public static Set<String> collectImmediateFieldNames(
      @Nonnull final DataFetchingEnvironment environment, @Nonnull final String objectTypeName) {
    final Map<String, FragmentDefinition> fragments =
        environment.getFragmentsByName() == null
            ? Collections.emptyMap()
            : environment.getFragmentsByName();
    final GraphQLSchema schema = environment.getGraphQLSchema();
    final Set<String> fieldNames = new LinkedHashSet<>();
    final Set<String> visitedFragments = new HashSet<>();

    for (Field astField : mergedAstFields(environment)) {
      collect(
          astField.getSelectionSet(),
          schema,
          fragments,
          objectTypeName,
          fieldNames,
          visitedFragments);
    }
    return fieldNames;
  }

  @Nonnull
  private static Iterable<Field> mergedAstFields(
      @Nonnull final DataFetchingEnvironment environment) {
    // Merged fields cover selections that share a response key (e.g. the same field repeated across
    // fragments). Aliased siblings have distinct response keys and are resolved separately, each
    // contributing its own selection to the request-scoped union.
    if (environment.getMergedField() != null) {
      return environment.getMergedField().getFields();
    }
    return environment.getField() == null
        ? Collections.emptyList()
        : Collections.singletonList(environment.getField());
  }

  private static void collect(
      @Nullable final SelectionSet selectionSet,
      @Nullable final GraphQLSchema schema,
      @Nonnull final Map<String, FragmentDefinition> fragments,
      @Nonnull final String objectTypeName,
      @Nonnull final Set<String> fieldNames,
      @Nonnull final Set<String> visitedFragments) {
    if (selectionSet == null) {
      return;
    }
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field) {
        fieldNames.add(((Field) selection).getName());
      } else if (selection instanceof InlineFragment) {
        final InlineFragment inlineFragment = (InlineFragment) selection;
        final TypeName typeCondition = inlineFragment.getTypeCondition();
        if (typeCondition == null || appliesTo(schema, typeCondition.getName(), objectTypeName)) {
          collect(
              inlineFragment.getSelectionSet(),
              schema,
              fragments,
              objectTypeName,
              fieldNames,
              visitedFragments);
        }
      } else if (selection instanceof FragmentSpread) {
        final FragmentSpread fragmentSpread = (FragmentSpread) selection;
        // Names are collected into a set, so visiting a fragment once is sufficient. This also
        // guards against cyclic spreads.
        if (!visitedFragments.add(fragmentSpread.getName())) {
          continue;
        }
        final FragmentDefinition definition = fragments.get(fragmentSpread.getName());
        if (definition != null
            && appliesTo(schema, definition.getTypeCondition().getName(), objectTypeName)) {
          collect(
              definition.getSelectionSet(),
              schema,
              fragments,
              objectTypeName,
              fieldNames,
              visitedFragments);
        }
      }
    }
  }

  /**
   * Whether a fragment type condition applies to the concrete object type being resolved. Unknown
   * types are treated as applicable so an unresolvable condition widens the aspect set rather than
   * dropping fields the mapper needs.
   */
  private static boolean appliesTo(
      @Nullable final GraphQLSchema schema,
      @Nonnull final String conditionTypeName,
      @Nonnull final String objectTypeName) {
    if (conditionTypeName.equals(objectTypeName)) {
      return true;
    }
    if (schema == null) {
      return true;
    }
    final GraphQLType conditionType = schema.getType(conditionTypeName);
    if (conditionType == null) {
      return true;
    }
    if (conditionType instanceof GraphQLInterfaceType) {
      return schema.getImplementations((GraphQLInterfaceType) conditionType).stream()
          .map(GraphQLObjectType::getName)
          .anyMatch(objectTypeName::equals);
    }
    if (conditionType instanceof GraphQLUnionType) {
      return ((GraphQLUnionType) conditionType)
          .getTypes().stream().map(GraphQLNamedType::getName).anyMatch(objectTypeName::equals);
    }
    // A concrete object type condition that is not this type contributes nothing.
    return false;
  }
}
