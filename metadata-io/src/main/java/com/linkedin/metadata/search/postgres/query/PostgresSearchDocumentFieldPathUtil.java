package com.linkedin.metadata.search.postgres.query;

import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Resolves logical pgSearch / OpenSearch filter field names to JSON {@code #>>} paths under {@code
 * document}, mirroring v3 index layout ({@code _aspects.<aspect>.<field>}) as in {@link
 * com.linkedin.metadata.search.elasticsearch.index.entity.v3.MultiEntityMappingsBuilder#collectFieldPathsFromAspect}.
 */
public final class PostgresSearchDocumentFieldPathUtil {

  private static final String ASPECTS_KEY = "_aspects";
  private static final String STRUCTURED_PROPERTIES = "structuredProperties";

  private PostgresSearchDocumentFieldPathUtil() {}

  /**
   * SQL expression returning text at the resolved path in {@code document} (single path or {@code
   * COALESCE} of multiple distinct paths when the same logical name maps to different aspects
   * across entity types).
   */
  @Nonnull
  public static String toDocumentTextSqlExpr(
      @Nonnull EntityRegistry registry,
      @Nonnull String tableAlias,
      @Nonnull String field,
      @Nullable List<String> entityTypesForResolution) {
    String normalized = PostgresSearchFilterSqlBuilder.normalizeFieldForJson(field);
    if (skipRegistryResolution(normalized, entityTypesForResolution)) {
      return PostgresSearchFilterSqlBuilder.documentTextPathExpr(tableAlias, normalized);
    }
    List<List<String>> paths =
        resolveSearchableFieldPaths(registry, entityTypesForResolution, normalized);
    if (paths.isEmpty()) {
      return PostgresSearchFilterSqlBuilder.documentTextPathExpr(tableAlias, normalized);
    }
    if (paths.size() == 1) {
      return documentTextPathExprFromSegments(tableAlias, paths.get(0));
    }
    return "COALESCE("
        + paths.stream()
            .map(segments -> documentTextPathExprFromSegments(tableAlias, segments))
            .collect(Collectors.joining(", "))
        + ")";
  }

  /**
   * SQL expression returning {@code jsonb} at the resolved path in {@code document} (single path or
   * {@code COALESCE} of multiple paths), for aggregations that need Elasticsearch-style array
   * expansion ({@code jsonb_array_elements_text}) instead of {@link #toDocumentTextSqlExpr} {@code
   * #>>} text.
   */
  @Nonnull
  public static String toDocumentJsonbSqlExpr(
      @Nonnull EntityRegistry registry,
      @Nonnull String tableAlias,
      @Nonnull String field,
      @Nullable List<String> entityTypesForResolution) {
    String normalized = PostgresSearchFilterSqlBuilder.normalizeFieldForJson(field);
    if (skipRegistryResolution(normalized, entityTypesForResolution)) {
      return PostgresSearchFilterSqlBuilder.documentJsonbPathExpr(tableAlias, normalized);
    }
    List<List<String>> paths =
        resolveSearchableFieldPaths(registry, entityTypesForResolution, normalized);
    if (paths.isEmpty()) {
      return PostgresSearchFilterSqlBuilder.documentJsonbPathExpr(tableAlias, normalized);
    }
    if (paths.size() == 1) {
      return documentJsonbPathExprFromSegments(tableAlias, paths.get(0));
    }
    return "COALESCE("
        + paths.stream()
            .map(segments -> documentJsonbPathExprFromSegments(tableAlias, segments))
            .collect(Collectors.joining(", "))
        + ")";
  }

  public static boolean skipRegistryResolution(
      @Nonnull String normalizedField, @Nullable List<String> entityTypes) {
    if (entityTypes == null || entityTypes.isEmpty()) {
      return true;
    }
    if (normalizedField.startsWith(ASPECTS_KEY + ".")) {
      return true;
    }
    if (normalizedField.startsWith(STRUCTURED_PROPERTIES + ".")) {
      return true;
    }
    return false;
  }

  /**
   * Collects distinct {@code _aspects.<aspect>.<searchableFieldName>} segment lists for a logical
   * criterion field across the given entity types.
   */
  @Nonnull
  static List<List<String>> resolveSearchableFieldPaths(
      @Nonnull EntityRegistry registry,
      @Nonnull List<String> entityTypeNames,
      @Nonnull String normalizedCriterionField) {
    Map<String, List<String>> orderedUnique = new LinkedHashMap<>();
    for (String entityName : entityTypeNames) {
      EntitySpec entitySpec = registry.getEntitySpec(entityName);
      if (entitySpec == null) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        if (STRUCTURED_PROPERTIES.equals(aspectSpec.getName())) {
          continue;
        }
        for (SearchableFieldSpec searchableFieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          SearchableAnnotation ann = searchableFieldSpec.getSearchableAnnotation();
          if (OBJECT_FIELD_TYPES.contains(ann.getFieldType())) {
            continue;
          }
          if (!matchesCriterionField(ann, normalizedCriterionField)) {
            continue;
          }
          List<String> segments = List.of(ASPECTS_KEY, aspectSpec.getName(), ann.getFieldName());
          orderedUnique.putIfAbsent(String.join(".", segments), segments);
        }
      }
    }
    return new ArrayList<>(orderedUnique.values());
  }

  private static boolean matchesCriterionField(
      SearchableAnnotation ann, String normalizedCriterionField) {
    if (normalizedCriterionField.equals(ann.getFieldName())) {
      return true;
    }
    if (normalizedCriterionField.equals(ann.getFilterName())) {
      return true;
    }
    if (ann.getSearchLabel().isPresent()
        && normalizedCriterionField.equals(ann.getSearchLabel().get())) {
      return true;
    }
    if (ann.getEntityFieldName().isPresent()
        && normalizedCriterionField.equals(ann.getEntityFieldName().get())) {
      return true;
    }
    if (ann.getFieldNameAliases() != null) {
      for (String alias : ann.getFieldNameAliases()) {
        if (normalizedCriterionField.equals(alias)) {
          return true;
        }
      }
    }
    return false;
  }

  @Nonnull
  static String documentTextPathExprFromSegments(
      @Nonnull String tableAlias, @Nonnull List<String> segments) {
    String array =
        "ARRAY['"
            + segments.stream().map(s -> s.replace("'", "''")).collect(Collectors.joining("','"))
            + "']::text[]";
    return tableAlias + ".document #>> " + array;
  }

  @Nonnull
  static String documentJsonbPathExprFromSegments(
      @Nonnull String tableAlias, @Nonnull List<String> segments) {
    String array =
        "ARRAY['"
            + segments.stream().map(s -> s.replace("'", "''")).collect(Collectors.joining("','"))
            + "']::text[]";
    return tableAlias + ".document #> " + array;
  }
}
