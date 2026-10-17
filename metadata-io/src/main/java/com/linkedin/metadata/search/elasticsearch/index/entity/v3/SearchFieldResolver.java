package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.models.annotation.v3.FieldMapping.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Resolves API field names to the representations Search V3 actually maps.
 *
 * <p>The resolver is immutable and built once with a cached request handler. It deliberately uses
 * registry annotations rather than inspecting live engine mappings, so query construction and
 * mapping generation follow the same contract.
 */
public final class SearchFieldResolver {

  public enum Intent {
    FILTER,
    FACET,
    SORT,
    RANGE,
    EXACT,
    FULL_TEXT,
    CONTAIN
  }

  private final Map<String, Field> fields;
  private final Set<String> sharedHasValues;

  public SearchFieldResolver(@Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, MutableField> byName = new LinkedHashMap<>();
    Map<String, List<SearchableFieldSpec>> byLabel = new LinkedHashMap<>();
    Set<String> sharedHasValuesNames = new LinkedHashSet<>();

    entitySpecs.stream()
        .flatMap(entity -> entity.getSearchableFieldSpecs().stream())
        .filter(spec -> spec.getSearchableAnnotation().isV3Searchable())
        .forEach(
            spec -> {
              SearchableAnnotation annotation = spec.getSearchableAnnotation().resolvedForV3();
              FieldMapping root = FieldMapping.rootOf(FieldMapping.of(annotation));
              MutableField source =
                  byName.computeIfAbsent(
                      annotation.getFieldName(),
                      ignored -> new MutableField(annotation.getFieldName()));
              source.addRoot(root);
              source.addSource(annotation.getFieldName());
              annotation.getHasValuesFieldName().ifPresent(source::setHasValues);
              Optional.ofNullable(annotation.getFieldNameAliases())
                  .orElse(List.of())
                  .forEach(
                      alias ->
                          byName
                              .computeIfAbsent(
                                  alias, ignored -> new MutableField(annotation.getFieldName()))
                              .merge(source));
              annotation
                  .getHasValuesFieldName()
                  .ifPresent(
                      field ->
                          byName
                              .computeIfAbsent(field, ignored -> new MutableField(field))
                              .addRoot(FieldMapping.root(Type.BOOLEAN)));
              annotation
                  .getNumValuesFieldName()
                  .ifPresent(
                      field ->
                          byName
                              .computeIfAbsent(field, ignored -> new MutableField(field))
                              .addRoot(FieldMapping.root(Type.COUNT)));
              annotation
                  .getSearchLabel()
                  .filter(label -> !label.isEmpty())
                  .ifPresent(
                      label -> {
                        byLabel.computeIfAbsent(label, ignored -> new ArrayList<>()).add(spec);
                        annotation.getHasValuesFieldName().ifPresent(sharedHasValuesNames::add);
                      });
            });

    byLabel.forEach(
        (label, contributors) -> {
          List<List<FieldMapping>> mappings =
              contributors.stream()
                  .map(SearchableFieldSpec::getSearchableAnnotation)
                  .map(SearchableAnnotation::resolvedForV3)
                  .map(FieldMapping::of)
                  .toList();
          FieldMapping.Resolved resolved = FieldMapping.union(label, mappings);
          MutableField field = byName.computeIfAbsent(label, MutableField::new);
          field.addRoot(resolved.getRoot());
          resolved.getSubfields().forEach(field::addSubfield);
          contributors.stream()
              .map(SearchableFieldSpec::getSearchableAnnotation)
              .map(SearchableAnnotation::resolvedForV3)
              .forEach(
                  annotation -> {
                    field.addSource(annotation.getFieldName());
                    annotation.getHasValuesFieldName().ifPresent(field::setHasValues);
                    byName.put(annotation.getFieldName(), field);
                    Optional.ofNullable(annotation.getFieldNameAliases())
                        .orElse(List.of())
                        .forEach(alias -> byName.put(alias, field));
                  });
        });

    for (String field : MappingConstants.SYSTEM_KEYWORD_FIELDS) {
      if (!byName.containsKey(field)) {
        MutableField direct = new MutableField(field);
        // System keyword fields have no case-preserving .keyword multi-field.
        direct.addRoot(FieldMapping.root(Type.URN));
        byName.put(field, direct);
      }
    }

    Map<String, Field> resolved = new LinkedHashMap<>();
    byName.forEach((name, field) -> resolved.put(name, field.toField()));
    fields = Collections.unmodifiableMap(resolved);
    sharedHasValues = Set.copyOf(sharedHasValuesNames);
  }

  /** A shared-label hasValues field stores only true. False is the field being missing. */
  public boolean falseIsMissing(@Nonnull String fieldName) {
    return sharedHasValues.contains(parent(fieldName));
  }

  /**
   * The boolean a label writes when a source is populated, such as {@code hasDescription} for
   * {@code description}. Empty when the field declares none.
   */
  @Nonnull
  public Optional<String> hasValuesFieldName(@Nonnull String fieldName) {
    Field field = fields.get(parent(fieldName));
    if (field == null || field.getHasValuesFieldName() == null) {
      return Optional.empty();
    }
    return Optional.of(field.getHasValuesFieldName());
  }

  public boolean contains(@Nonnull String fieldName) {
    return fields.containsKey(parent(fieldName));
  }

  public boolean supports(@Nonnull String fieldName, @Nonnull Intent intent) {
    Field field = fields.get(parent(fieldName));
    return field != null && field.supports(intent);
  }

  @Nonnull
  public Field resolve(@Nonnull String fieldName, @Nonnull Intent intent) {
    String parent = parent(fieldName);
    Field field =
        Optional.ofNullable(fields.get(parent))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Search V3 field '"
                            + fieldName
                            + "' is not declared in the entity registry"));
    if (!field.supports(intent)) {
      throw new IllegalArgumentException(
          "Search V3 field '"
              + fieldName
              + "' does not support "
              + intent.name().toLowerCase()
              + " queries");
    }
    return field;
  }

  @Nonnull
  public String exactPath(@Nonnull String fieldName, @Nonnull Intent intent) {
    Field field = resolve(fieldName, intent);
    return field.isKeywordSubfield() && intent != Intent.SORT
        ? field.getPath() + ".keyword"
        : field.getPath();
  }

  /**
   * Analyzed subfields of a text-only label. A contains filter matches these fields. An indexed
   * label contains on its keyword path instead.
   */
  @Nonnull
  public List<String> analyzedPaths(@Nonnull String fieldName) {
    Field field = resolve(fieldName, Intent.CONTAIN);
    if (field.isIndexed()) {
      throw new IllegalArgumentException(
          "Search V3 field '" + fieldName + "' contains on its keyword field");
    }
    List<String> paths = new ArrayList<>();
    if (field.getSubfields().contains(V3SearchFields.TEXT)) {
      paths.add(field.getPath() + "." + V3SearchFields.TEXT);
    }
    if (field.getSubfields().contains(V3SearchFields.STEMMED)) {
      paths.add(field.getPath() + "." + V3SearchFields.STEMMED);
    }
    return List.copyOf(paths);
  }

  @Nonnull
  public Set<String> sourceFields(@Nonnull String fieldName) {
    return resolve(fieldName, Intent.FILTER).getSourceFields();
  }

  private static String parent(String fieldName) {
    for (String suffix :
        List.of(
            ".keyword",
            "." + V3SearchFields.TEXT,
            "." + V3SearchFields.STEMMED,
            ".ngram",
            ".delimited",
            ".wordGrams2")) {
      if (fieldName.endsWith(suffix)) {
        return fieldName.substring(0, fieldName.length() - suffix.length());
      }
    }
    return fieldName;
  }

  @Value
  public static class Field {
    @Nonnull String path;
    @Nonnull Type rootType;
    boolean indexed;
    boolean docValues;
    boolean keywordSubfield;
    @Nonnull Set<String> subfields;
    @Nonnull Set<String> sourceFields;
    @Nonnull Set<Intent> supportedIntents;
    @Nullable String hasValuesFieldName;

    boolean supports(Intent intent) {
      return supportedIntents.contains(intent);
    }
  }

  private static final class MutableField {
    private final String path;
    private FieldMapping root;
    private final Set<String> subfields = new LinkedHashSet<>();
    private final Set<String> sources = new LinkedHashSet<>();
    private String hasValuesFieldName;

    private MutableField(String path) {
      this.path = path;
    }

    private void addRoot(FieldMapping candidate) {
      if (root != null && !root.sameRoot(candidate)) {
        throw new IllegalArgumentException(
            "Search V3 field '" + path + "' resolves to incompatible exact roots");
      }
      root = candidate;
    }

    private void addSubfield(FieldMapping subfield) {
      subfields.add(subfield.getSubfield());
    }

    private void addSource(String source) {
      sources.add(source);
    }

    private void setHasValues(String name) {
      if (hasValuesFieldName != null && !hasValuesFieldName.equals(name)) {
        throw new IllegalArgumentException(
            "Search V3 field '"
                + path
                + "' declares both '"
                + hasValuesFieldName
                + "' and '"
                + name
                + "' as hasValuesFieldName");
      }
      hasValuesFieldName = name;
    }

    private void merge(MutableField source) {
      if (source.root != null) {
        addRoot(source.root);
      }
      subfields.addAll(source.subfields);
      sources.addAll(source.sources);
      if (source.hasValuesFieldName != null) {
        setHasValues(source.hasValuesFieldName);
      }
    }

    private Field toField() {
      FieldMapping resolvedRoot =
          Optional.ofNullable(root).orElseGet(() -> FieldMapping.root(Type.KEYWORD));
      EnumSet<Intent> intents = EnumSet.noneOf(Intent.class);
      boolean analyzed =
          subfields.contains(V3SearchFields.TEXT) || subfields.contains(V3SearchFields.STEMMED);
      if (resolvedRoot.isIndex()) {
        intents.add(Intent.FILTER);
        intents.add(Intent.EXACT);
        intents.add(Intent.RANGE);
        intents.add(Intent.CONTAIN);
      } else if (analyzed) {
        intents.add(Intent.CONTAIN);
      }
      if (resolvedRoot.isDocValues()) {
        intents.add(Intent.FACET);
        intents.add(Intent.SORT);
      }
      if (analyzed) {
        intents.add(Intent.FULL_TEXT);
      }
      return new Field(
          path,
          resolvedRoot.getType(),
          resolvedRoot.isIndex(),
          resolvedRoot.isDocValues(),
          resolvedRoot.getType() == Type.KEYWORD && resolvedRoot.isIndex(),
          Set.copyOf(subfields),
          Set.copyOf(sources),
          Set.copyOf(intents),
          hasValuesFieldName);
    }
  }
}
