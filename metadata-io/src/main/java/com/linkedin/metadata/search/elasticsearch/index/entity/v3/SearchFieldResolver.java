package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.models.annotation.v3.FieldMapping.Type;
import com.linkedin.metadata.search.utils.ESUtils;
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
  private final Map<String, Field> exactSourceFields;
  private final Set<String> sharedHasValues;

  public SearchFieldResolver(@Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, MutableField> byName = new LinkedHashMap<>();
    Map<String, MutableField> sources = new LinkedHashMap<>();
    Map<String, List<SearchableFieldSpec>> byLabel = new LinkedHashMap<>();
    Map<String, List<String>> labelContributors = new LinkedHashMap<>();
    Set<String> sharedHasValuesNames = new LinkedHashSet<>();
    Set<String> splitSourceNames = new LinkedHashSet<>();
    Map<String, MutableField> aliasedTo = new LinkedHashMap<>();

    for (EntitySpec entity : entitySpecs) {
      List<AspectSpec> aspects = entity.getAspectSpecs();
      if (aspects == null || aspects.isEmpty()) {
        // Specs built without aspects, including resolver tests, expose fields directly.
        for (SearchableFieldSpec spec : entity.getSearchableFieldSpecs()) {
          indexSearchableField(
              entity,
              null,
              spec,
              byName,
              sources,
              byLabel,
              labelContributors,
              sharedHasValuesNames);
        }
      } else {
        for (AspectSpec aspect : aspects) {
          for (SearchableFieldSpec spec : aspect.getSearchableFieldSpecs()) {
            indexSearchableField(
                entity,
                aspect,
                spec,
                byName,
                sources,
                byLabel,
                labelContributors,
                sharedHasValuesNames);
          }
        }
      }
    }

    byLabel.forEach(
        (label, contributors) -> {
          List<List<FieldMapping>> mappings =
              contributors.stream().map(V3SearchFields::labelMappings).toList();
          FieldMapping.Resolved resolved = FieldMapping.union(label, mappings);
          MutableField field = byName.computeIfAbsent(label, MutableField::new);
          field.addRoot(
              resolved.getRoot(),
              "indexedField="
                  + label
                  + " "
                  + resolved.getRoot().getType()
                  + " index="
                  + resolved.getRoot().isIndex()
                  + " docValues="
                  + resolved.getRoot().isDocValues()
                  + " ["
                  + String.join("; ", labelContributors.getOrDefault(label, List.of()))
                  + "]");
          resolved.getSubfields().forEach(field::addSubfield);
          contributors.stream()
              .map(SearchableFieldSpec::getSearchableAnnotation)
              .map(SearchableAnnotation::resolvedForV3)
              .forEach(
                  annotation -> {
                    field.addSource(annotation.getFieldName());
                    labelHasValues(annotation).ifPresent(field::setHasValues);
                    aliasSource(
                        byName,
                        sources,
                        aliasedTo,
                        splitSourceNames,
                        annotation.getFieldName(),
                        field);
                    Optional.ofNullable(annotation.getFieldNameAliases())
                        .orElse(List.of())
                        .forEach(
                            alias ->
                                aliasSource(
                                    byName, sources, aliasedTo, splitSourceNames, alias, field));
                  });
        });

    for (String field : MappingConstants.SYSTEM_KEYWORD_FIELDS) {
      if (!byName.containsKey(field)) {
        MutableField direct = new MutableField(field);
        // System keyword fields have no case-preserving .keyword multi-field.
        direct.addRoot(FieldMapping.root(Type.URN), "system keyword " + field);
        byName.put(field, direct);
      }
    }
    parsedKeywordPaths(entitySpecs)
        .forEach(
            (name, paths) -> {
              MutableField field =
                  byName.computeIfAbsent(
                      name,
                      key -> {
                        MutableField created = new MutableField(key);
                        created.addRoot(
                            FieldMapping.root(Type.KEYWORD, false, false), "parsed keyword " + key);
                        created.addSubfield(FieldMapping.subfield(V3SearchFields.TEXT, Type.TEXT));
                        created.addSubfield(
                            FieldMapping.subfield(V3SearchFields.STEMMED, Type.STEMMED));
                        return created;
                      });
              field.parsedExactPaths.addAll(paths);
              if (V3SearchFields.isParsedUrnRoot(name, entitySpecs)) {
                field.parsedKeywordLabel = true;
                if (V3SearchFields.urnRootHasAnalyzers(name, entitySpecs)) {
                  field.addSubfield(FieldMapping.subfield(V3SearchFields.TEXT, Type.TEXT));
                  field.addSubfield(FieldMapping.subfield(V3SearchFields.STEMMED, Type.STEMMED));
                }
              }
            });

    Map<String, Field> resolved = new LinkedHashMap<>();
    byName.forEach((name, field) -> resolved.put(name, field.toField()));
    fields = Collections.unmodifiableMap(resolved);
    Map<String, Field> exactSources = new LinkedHashMap<>();
    sources.forEach(
        (name, field) -> {
          if (!V3SearchFields.omitsSourceRoot(name, entitySpecs)) {
            exactSources.put(name, field.toField());
          }
        });
    exactSourceFields = Collections.unmodifiableMap(exactSources);
    sharedHasValues = Set.copyOf(sharedHasValuesNames);
  }

  /** A shared-label hasValues field stores only true. False is the field being missing. */
  public boolean falseIsMissing(@Nonnull String fieldName) {
    return sharedHasValues.contains(parent(fieldName));
  }

  /**
   * The boolean a label writes when a source is populated. Degree 0 is {@code hasDescription} for
   * {@code description}. Degree 1 is {@code hasDescription_1} for {@code description_1}. Empty when
   * the field declares none or the source spans multiple degree-specific labels.
   */
  @Nonnull
  public Optional<String> hasValuesFieldName(@Nonnull String fieldName) {
    Field field = fields.get(parent(fieldName));
    if (field == null || field.getHasValuesFieldNames().size() != 1) {
      return Optional.empty();
    }
    return Optional.of(field.getHasValuesFieldNames().get(0));
  }

  /**
   * The booleans written when a source is populated. A source shared across entity types can point
   * at more than one degree-specific label and therefore more than one boolean.
   */
  @Nonnull
  public List<String> hasValuesFieldNames(@Nonnull String fieldName) {
    Field field = fields.get(parent(fieldName));
    return field == null ? List.of() : field.getHasValuesFieldNames();
  }

  public boolean contains(@Nonnull String fieldName) {
    return fields.containsKey(parent(fieldName));
  }

  public boolean supports(@Nonnull String fieldName, @Nonnull Intent intent) {
    Field field = selectField(parent(fieldName), intent);
    return field != null && field.supports(intent);
  }

  @Nonnull
  public Field resolve(@Nonnull String fieldName, @Nonnull Intent intent) {
    String parent = parent(fieldName);
    Field field =
        Optional.ofNullable(selectField(parent, intent))
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

  @Nullable
  private Field selectField(@Nonnull String fieldName, @Nonnull Intent intent) {
    // Exact-match scoring may still target a retained source root directly. Criteria, facets, and
    // sorts select the shared label that represents the concept across entity types.
    if (intent == Intent.EXACT && exactSourceFields.containsKey(fieldName)) {
      return exactSourceFields.get(fieldName);
    }
    return fields.get(fieldName);
  }

  @Nonnull
  public String exactPath(@Nonnull String fieldName, @Nonnull Intent intent) {
    Field field = resolve(fieldName, intent);
    // The logical field stores the parsed name. The complete URN is the <field>_urn sibling.
    if (field.isParsedKeywordLabel()) {
      if (intent == Intent.EXACT || intent == Intent.FULL_TEXT) {
        return field.getPath();
      }
      return V3SearchFields.fullUrnField(field.getPath());
    }
    return field.isKeywordSubfield() && intent != Intent.SORT
        ? field.getPath() + ESUtils.KEYWORD_SUFFIX
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
    if (!field.getAnalyzedPaths().isEmpty()) {
      return field.getAnalyzedPaths();
    }
    List<String> paths = new ArrayList<>();
    if (field.getSubfields().contains(V3SearchFields.TEXT)) {
      paths.add(field.getPath() + V3SearchFields.TEXT_SUFFIX);
    }
    if (field.getSubfields().contains(V3SearchFields.STEMMED)) {
      paths.add(field.getPath() + V3SearchFields.STEMMED_SUFFIX);
    }
    return List.copyOf(paths);
  }

  /**
   * Case-folded complete URN. A parsed root reads {@code <field>_urn.lowercase}. The document URN
   * reads {@code urn.lowercase}, and its parsed identity reads {@code urn_name.lowercase}. Empty
   * when the field has no case-folded URN.
   */
  @Nonnull
  public String lowercaseFullUrnPath(@Nonnull String fieldName) {
    Field field = fields.get(parent(fieldName));
    if (field == null) {
      return "";
    }
    if (field.isParsedKeywordLabel()) {
      return V3SearchFields.fullUrnLowercaseField(field.getPath());
    }
    if (MappingConstants.URN_FIELD.equals(field.getPath())
        || MappingConstants.URN_NAME_FIELD.equals(field.getPath())) {
      return field.getPath() + "." + V3SearchFields.LOWERCASE;
    }
    return "";
  }

  /**
   * Parsed keywords exact match reads for this field. A URN root points at the logical field,
   * including a field such as {@code owners} that has no analyzers. {@code other} points at each
   * contributing aspect keyword.
   */
  @Nonnull
  public List<String> parsedKeywordPaths(@Nonnull String fieldName) {
    Field field = fields.get(parent(fieldName));
    if (field == null) {
      return List.of();
    }
    return field.getParsedExactPaths();
  }

  /**
   * Exact keywords for a full-text URN. A URN label points at the parsed root. {@code other} points
   * at each contributing aspect keyword.
   */
  @Nonnull
  private static Map<String, List<String>> parsedKeywordPaths(
      @Nonnull Collection<EntitySpec> entitySpecs) {
    Map<String, List<String>> paths = new LinkedHashMap<>();
    for (EntitySpec entity : entitySpecs) {
      if (entity.getAspectSpecs() == null) {
        continue;
      }
      for (AspectSpec aspect : entity.getAspectSpecs()) {
        for (SearchableFieldSpec spec : aspect.getSearchableFieldSpecs()) {
          if (!V3SearchFields.contributesParsedUrn(spec)) {
            continue;
          }
          String fieldName = spec.getSearchableAnnotation().resolvedForV3().getFieldName();
          String aspectParsed =
              MappingConstants.ASPECTS_FIELD_NAME
                  + "."
                  + aspect.getName()
                  + "."
                  + V3SearchFields.parsedUrnField(fieldName);
          List<String> destinations = V3SearchFields.parsedNameDestinations(spec, entitySpecs);
          if (destinations.isEmpty()) {
            if (!V3SearchFields.contributesFullTextUrn(spec)) {
              continue;
            }
            destinations = List.of(fieldName);
          }
          for (String destination : destinations) {
            boolean parsedRoot = V3SearchFields.isParsedUrnRoot(destination, entitySpecs);
            String exact = parsedRoot ? destination : aspectParsed;
            List<String> exactPaths =
                paths.computeIfAbsent(destination, ignored -> new ArrayList<>());
            if (!exactPaths.contains(exact)) {
              exactPaths.add(exact);
            }
          }
        }
      }
    }
    return paths;
  }

  @Nonnull
  public Set<String> sourceFields(@Nonnull String fieldName) {
    return resolve(fieldName, Intent.FILTER).getSourceFields();
  }

  /**
   * A V2 field name follows its shared label when every labeled contributor agrees. The shared
   * mapping copies an unlabeled root of the same name into that label, so the label represents the
   * concept across entity types. The same name at two degrees stays the source field, so a filter
   * on it does not pick one degree for every entity.
   */
  private static void aliasSource(
      Map<String, MutableField> byName,
      Map<String, MutableField> sources,
      Map<String, MutableField> aliasedTo,
      Set<String> splitSourceNames,
      String name,
      MutableField indexedField) {
    if (splitSourceNames.contains(name)) {
      MutableField source = sources.get(name);
      if (source != null) {
        source.addSearchTarget(indexedField);
      }
      return;
    }
    MutableField prior = aliasedTo.get(name);
    if (prior != null && prior != indexedField) {
      splitSourceNames.add(name);
      MutableField source = sources.get(name);
      if (source != null) {
        source.addSearchTarget(prior);
        source.addSearchTarget(indexedField);
        byName.put(name, source);
      } else {
        byName.remove(name);
      }
      return;
    }
    aliasedTo.put(name, indexedField);
    byName.put(name, indexedField);
  }

  private static String parent(String fieldName) {
    String current = fieldName;
    boolean stripped = true;
    while (stripped) {
      stripped = false;
      for (String suffix :
          List.of(
              ESUtils.KEYWORD_SUFFIX,
              V3SearchFields.TEXT_SUFFIX,
              V3SearchFields.STEMMED_SUFFIX,
              "." + V3SearchFields.PARSED_KEYWORD,
              "." + V3SearchFields.LOWERCASE,
              ".ngram",
              ".delimited",
              ".wordGrams2")) {
        if (current.endsWith(suffix) && current.length() > suffix.length()) {
          current = current.substring(0, current.length() - suffix.length());
          stripped = true;
          break;
        }
      }
    }
    return current;
  }

  @Value
  public static class Field {
    @Nonnull String path;
    @Nonnull List<String> parsedExactPaths;
    @Nonnull Type rootType;
    boolean indexed;
    boolean docValues;
    boolean keywordSubfield;
    boolean parsedKeywordLabel;
    @Nonnull Set<String> subfields;
    @Nonnull Set<String> sourceFields;
    @Nonnull Set<Intent> supportedIntents;
    @Nonnull List<String> hasValuesFieldNames;
    @Nonnull List<String> analyzedPaths;

    boolean supports(Intent intent) {
      return supportedIntents.contains(intent);
    }
  }

  private static void indexSearchableField(
      EntitySpec entity,
      @Nullable AspectSpec aspect,
      SearchableFieldSpec spec,
      Map<String, MutableField> byName,
      Map<String, MutableField> sources,
      Map<String, List<SearchableFieldSpec>> byLabel,
      Map<String, List<String>> labelContributors,
      Set<String> sharedHasValuesNames) {
    if (!spec.getSearchableAnnotation().isV3Searchable()) {
      return;
    }
    SearchableAnnotation annotation = spec.getSearchableAnnotation().resolvedForV3();
    FieldMapping root = FieldMapping.rootOf(FieldMapping.of(annotation));
    String described = contributor(entity, aspect, spec, annotation, root);
    MutableField source =
        byName.computeIfAbsent(
            annotation.getFieldName(), ignored -> new MutableField(annotation.getFieldName()));
    source.addRoot(root, described);
    source.addSource(annotation.getFieldName());
    annotation.getHasValuesFieldName().ifPresent(source::setHasValues);
    sources.put(annotation.getFieldName(), source);
    Optional.ofNullable(annotation.getFieldNameAliases())
        .orElse(List.of())
        .forEach(
            alias ->
                byName
                    .computeIfAbsent(alias, ignored -> new MutableField(annotation.getFieldName()))
                    .merge(source));
    annotation
        .indexedHasValuesFieldName()
        .ifPresent(
            field ->
                byName
                    .computeIfAbsent(field, ignored -> new MutableField(field))
                    .addRoot(FieldMapping.root(Type.BOOLEAN), described + " hasValues=" + field));
    SearchableAnnotation.sharedHasValuesName(annotation)
        .ifPresent(
            field ->
                byName
                    .computeIfAbsent(field, ignored -> new MutableField(field))
                    .addRoot(FieldMapping.root(Type.BOOLEAN), described + " hasValues=" + field));
    annotation
        .getNumValuesFieldName()
        .ifPresent(
            field ->
                byName
                    .computeIfAbsent(field, ignored -> new MutableField(field))
                    .addRoot(FieldMapping.root(Type.COUNT), described + " numValues=" + field));
    annotation
        .indexedFieldName()
        .ifPresent(
            indexedName -> {
              byLabel.computeIfAbsent(indexedName, ignored -> new ArrayList<>()).add(spec);
              labelContributors
                  .computeIfAbsent(indexedName, ignored -> new ArrayList<>())
                  .add(described);
              annotation.indexedHasValuesFieldName().ifPresent(sharedHasValuesNames::add);
              SearchableAnnotation.sharedHasValuesName(annotation)
                  .ifPresent(sharedHasValuesNames::add);
            });
  }

  /** The boolean attached to a label: the shared V3 name when set, otherwise the top-level name. */
  @Nonnull
  private static Optional<String> labelHasValues(@Nonnull SearchableAnnotation annotation) {
    Optional<String> shared = SearchableAnnotation.sharedHasValuesName(annotation);
    if (shared.isPresent()) {
      return shared;
    }
    Optional<String> declared = annotation.indexedHasValuesFieldName();
    return declared == null ? Optional.empty() : declared;
  }

  private static String contributor(
      EntitySpec entity,
      @Nullable AspectSpec aspect,
      SearchableFieldSpec spec,
      SearchableAnnotation annotation,
      FieldMapping root) {
    String owner =
        aspect == null
            ? String.valueOf(entity.getName())
            : entity.getName() + "." + aspect.getName();
    return owner
        + " "
        + spec.getPath()
        + " fieldName="
        + annotation.getFieldName()
        + " fieldType="
        + annotation.getFieldType()
        + " "
        + root.getType()
        + " index="
        + root.isIndex()
        + " docValues="
        + root.isDocValues();
  }

  private static final class MutableField {
    private final String path;
    private FieldMapping root;
    private String rootContributor;
    private boolean parsedKeywordLabel;
    private final List<String> parsedExactPaths = new ArrayList<>();
    private final Set<String> subfields = new LinkedHashSet<>();
    private final Set<String> sources = new LinkedHashSet<>();
    private final Set<String> hasValuesFieldNames = new LinkedHashSet<>();
    private final Set<String> analyzedPaths = new LinkedHashSet<>();

    private MutableField(String path) {
      this.path = path;
    }

    private void addRoot(FieldMapping candidate, @Nullable String contributor) {
      if (root != null && !root.sameRoot(candidate)) {
        throw new IllegalArgumentException(
            "Search V3 field '"
                + path
                + "' resolves to incompatible exact roots: "
                + rootContributor
                + " vs "
                + contributor);
      }
      root = candidate;
      if (contributor != null) {
        rootContributor = contributor;
      }
    }

    private void addSubfield(FieldMapping subfield) {
      subfields.add(subfield.getSubfield());
    }

    private void addSource(String source) {
      sources.add(source);
    }

    private void setHasValues(String name) {
      if (!hasValuesFieldNames.isEmpty() && !hasValuesFieldNames.contains(name)) {
        throw new IllegalArgumentException(
            "Search V3 field '"
                + path
                + "' declares both '"
                + hasValuesFieldNames.iterator().next()
                + "' and '"
                + name
                + "' as hasValuesFieldName");
      }
      hasValuesFieldNames.add(name);
    }

    private void addSearchTarget(MutableField target) {
      hasValuesFieldNames.addAll(target.hasValuesFieldNames);
      if (target.subfields.contains(V3SearchFields.TEXT)) {
        analyzedPaths.add(target.path + "." + V3SearchFields.TEXT);
      }
      if (target.subfields.contains(V3SearchFields.STEMMED)) {
        analyzedPaths.add(target.path + "." + V3SearchFields.STEMMED);
      }
    }

    private void merge(MutableField source) {
      if (source.root != null) {
        addRoot(source.root, source.rootContributor);
      }
      subfields.addAll(source.subfields);
      sources.addAll(source.sources);
      hasValuesFieldNames.addAll(source.hasValuesFieldNames);
      analyzedPaths.addAll(source.analyzedPaths);
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
      if (!analyzedPaths.isEmpty()) {
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
          List.copyOf(parsedExactPaths),
          resolvedRoot.getType(),
          resolvedRoot.isIndex(),
          resolvedRoot.isDocValues(),
          resolvedRoot.getType() == Type.KEYWORD && resolvedRoot.isIndex(),
          parsedKeywordLabel,
          Set.copyOf(subfields),
          Set.copyOf(sources),
          Set.copyOf(intents),
          List.copyOf(hasValuesFieldNames),
          List.copyOf(analyzedPaths));
    }
  }
}
