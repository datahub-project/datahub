package com.linkedin.metadata.search.elasticsearch.query.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.models.LogicalValueType;
import com.linkedin.metadata.models.StructuredPropertyUtils;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Structured properties whose definition sets {@code queryByDefault}. The builder queries each
 * property's existing field. It does not copy values into a shared field. Name-and-value expansion
 * reads {@link #catalog()}, which is the string and rich-text definitions that survived field-name
 * collision. A definition the collision resolver omitted is not queried.
 */
public final class StructuredPropertyQueryFields {
  private static volatile List<SearchFieldConfig> fields = List.of();
  private static volatile Map<String, LogicalValueType> valueTypes = Map.of();
  private static volatile List<Definition> catalog = List.of();
  private static volatile Map<String, Set<String>> allowedEntities = Map.of();
  private static volatile Map<String, String> urnByPath = Map.of();
  private static volatile Map<String, String> pathByUrn = Map.of();
  private static volatile Set<String> omitted = Set.of();
  private static volatile String catalogStamp = stamp(List.of());

  private StructuredPropertyQueryFields() {}

  /**
   * One definition that can participate in name-and-value matching. Names stay here. They are not
   * copied onto the asset document.
   */
  public record Definition(
      @Nonnull String urn,
      @Nonnull String qualifiedName,
      @Nullable String displayName,
      @Nullable String version,
      @Nonnull LogicalValueType valueType,
      @Nonnull String fieldPath,
      @Nonnull Set<String> entityTypes) {}

  public static void replace(
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    if (properties == null || properties.isEmpty()) {
      fields = List.of();
      valueTypes = Map.of();
      catalog = List.of();
      allowedEntities = Map.of();
      urnByPath = Map.of();
      pathByUrn = Map.of();
      omitted = Set.of();
      catalogStamp = stamp(List.of());
      return;
    }
    List<Pair<Urn, StructuredPropertyDefinition>> ordered = new ArrayList<>();
    for (Pair<Urn, StructuredPropertyDefinition> pair : properties) {
      if (pair.getValue() != null) {
        ordered.add(pair);
      }
    }
    ordered.sort(Comparator.comparing(pair -> pair.getKey().toString()));
    Map<String, List<Pair<Urn, StructuredPropertyDefinition>>> byField = new LinkedHashMap<>();
    for (Pair<Urn, StructuredPropertyDefinition> pair : ordered) {
      String suffix = toElasticsearchFieldName(pair.getKey(), pair.getValue());
      byField.computeIfAbsent(suffix, key -> new ArrayList<>()).add(pair);
    }
    Set<String> dropped = new LinkedHashSet<>();
    Set<String> winners = new LinkedHashSet<>();
    for (List<Pair<Urn, StructuredPropertyDefinition>> group : byField.values()) {
      if (group.size() == 1) {
        winners.add(group.get(0).getKey().toString());
        continue;
      }
      LogicalValueType first = StructuredPropertyUtils.getLogicalValueType(group.get(0).getValue());
      boolean same =
          group.stream()
              .allMatch(
                  pair -> StructuredPropertyUtils.getLogicalValueType(pair.getValue()) == first);
      if (!same) {
        for (Pair<Urn, StructuredPropertyDefinition> pair : group) {
          dropped.add(path(pair.getKey(), pair.getValue()));
        }
        continue;
      }
      winners.add(group.get(0).getKey().toString());
    }

    List<SearchFieldConfig> next = new ArrayList<>();
    Map<String, LogicalValueType> types = new LinkedHashMap<>();
    Map<String, Set<String>> allowed = new LinkedHashMap<>();
    Map<String, String> urns = new LinkedHashMap<>();
    Map<String, String> paths = new LinkedHashMap<>();
    List<Definition> expandable = new ArrayList<>();
    for (Pair<Urn, StructuredPropertyDefinition> pair : ordered) {
      StructuredPropertyDefinition definition = pair.getValue();
      String fieldPath = path(pair.getKey(), definition);
      if (dropped.contains(fieldPath)) {
        continue;
      }
      if (!winners.contains(pair.getKey().toString())) {
        continue;
      }
      LogicalValueType type = StructuredPropertyUtils.getLogicalValueType(definition);
      types.put(fieldPath, type);
      Set<String> entityTypes = entityTypes(definition);
      allowed.put(fieldPath, entityTypes);
      urns.put(fieldPath, pair.getKey().toString());
      paths.put(pair.getKey().toString(), fieldPath);
      SearchFieldConfig field = queryByDefault(pair.getKey(), definition);
      if (field != null) {
        next.add(field);
      }
      if (type == LogicalValueType.STRING || type == LogicalValueType.RICH_TEXT) {
        expandable.add(
            new Definition(
                pair.getKey().toString(),
                definition.getQualifiedName(),
                definition.hasDisplayName() ? definition.getDisplayName() : null,
                definition.getVersion(GetMode.NULL),
                type,
                fieldPath,
                entityTypes));
      }
    }
    fields = List.copyOf(next);
    valueTypes = Map.copyOf(types);
    catalog = List.copyOf(expandable);
    allowedEntities = Map.copyOf(allowed);
    urnByPath = Map.copyOf(urns);
    pathByUrn = Map.copyOf(paths);
    omitted = Set.copyOf(dropped);
    catalogStamp = stamp(expandable);
  }

  /** String and rich-text definitions that remain after collision resolution. */
  @Nonnull
  public static List<Definition> catalog() {
    return catalog;
  }

  /** Hash of the string and rich-text catalog. A display-name change produces a different stamp. */
  @Nonnull
  public static String catalogStamp() {
    return catalogStamp;
  }

  /** True when conflicting mappings caused the collision resolver to omit this field path. */
  public static boolean omitted(@Nonnull String fieldPath) {
    return omitted.contains(fieldPath);
  }

  /** Definition URN for an indexed path, without a {@code .keyword} suffix. */
  @Nonnull
  public static Optional<String> definitionUrn(@Nonnull String fieldPath) {
    return Optional.ofNullable(urnByPath.get(stripKeyword(fieldPath)));
  }

  /** Indexed path for a definition URN. */
  @Nonnull
  public static Optional<String> fieldPath(@Nonnull String urn) {
    return Optional.ofNullable(pathByUrn.get(urn));
  }

  /** Logical type of an indexed structured-property path, without a {@code .keyword} suffix. */
  @Nonnull
  public static Optional<LogicalValueType> valueType(@Nonnull String fieldName) {
    String base = stripKeyword(fieldName);
    LogicalValueType known = valueTypes.get(base);
    if (known != null) {
      return Optional.of(known);
    }
    return StructuredPropertyUtils.getLogicalValueTypeFromFieldName(base);
  }

  @Nonnull
  public static List<SearchFieldConfig> fields() {
    return fields;
  }

  /**
   * Query-by-default fields allowed on at least one of {@code entityNames}. A definition with no
   * entity types is allowed on every entity. An omitted collision field is already absent.
   */
  @Nonnull
  public static List<SearchFieldConfig> fieldsFor(@Nonnull Collection<String> entityNames) {
    if (entityNames.isEmpty()) {
      return fields;
    }
    List<SearchFieldConfig> matched = new ArrayList<>();
    for (SearchFieldConfig field : fields) {
      if (allows(field.fieldName(), entityNames)) {
        matched.add(field);
      }
    }
    return matched;
  }

  public static boolean allows(@Nonnull String fieldPath, @Nonnull Collection<String> entityNames) {
    Set<String> allowed = allowedEntities.get(fieldPath);
    if (allowed == null || allowed.isEmpty() || entityNames.isEmpty()) {
      return true;
    }
    for (String entityName : entityNames) {
      if (allowed.contains(entityName)) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  public static SearchFieldConfig queryField(
      @Nonnull Urn urn, @Nullable StructuredPropertyDefinition definition) {
    SearchFieldConfig field = queryByDefault(urn, definition);
    if (field != null && omitted.contains(field.fieldName())) {
      return null;
    }
    return field;
  }

  @Nullable
  private static SearchFieldConfig queryByDefault(
      @Nonnull Urn urn, @Nullable StructuredPropertyDefinition definition) {
    if (definition == null || !definition.hasSearchConfiguration()) {
      return null;
    }
    DataHubSearchConfig config = definition.getSearchConfiguration();
    if (config == null || !config.isQueryByDefault()) {
      return null;
    }
    Double boost = config.getBoostScore();
    return SearchFieldConfig.detectSubFieldType(
        path(urn, definition),
        boost == null ? 1f : boost.floatValue(),
        SearchableAnnotation.FieldType.KEYWORD,
        true);
  }

  @Nonnull
  private static Set<String> entityTypes(@Nonnull StructuredPropertyDefinition definition) {
    if (!definition.hasEntityTypes() || definition.getEntityTypes() == null) {
      return Set.of();
    }
    Set<String> names = new LinkedHashSet<>();
    definition
        .getEntityTypes()
        .forEach(
            urn -> {
              String name = StructuredPropertyUtils.getEntityTypeId(urn);
              if (name != null && !name.isBlank()) {
                names.add(name);
              }
            });
    return Set.copyOf(names);
  }

  @Nonnull
  private static String stripKeyword(@Nonnull String fieldName) {
    if (fieldName.endsWith(".keyword")) {
      return fieldName.substring(0, fieldName.length() - ".keyword".length());
    }
    return fieldName;
  }

  @Nonnull
  private static String path(@Nonnull Urn urn, @Nonnull StructuredPropertyDefinition definition) {
    return STRUCTURED_PROPERTY_MAPPING_FIELD + "." + toElasticsearchFieldName(urn, definition);
  }

  @Nonnull
  private static String stamp(@Nonnull List<Definition> expandable) {
    List<String> lines = new ArrayList<>();
    for (Definition definition : expandable) {
      lines.add(
          String.join(
              "\u001f",
              definition.urn(),
              definition.qualifiedName(),
              definition.displayName() == null ? "" : definition.displayName(),
              definition.version() == null ? "" : definition.version(),
              definition.valueType().name()));
    }
    lines.sort(Comparator.naturalOrder());
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(String.join("\n", lines).getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
