package com.linkedin.metadata.search.elasticsearch.index.entity.v3;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.models.annotation.SearchableAnnotation.OBJECT_FIELD_TYPES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.ANALYZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.DOC_VALUES;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.KEYWORD_NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NORMALIZER;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.SEARCH_ANALYZER;
import static com.linkedin.metadata.search.utils.ESUtils.FIELDS;
import static com.linkedin.metadata.search.utils.ESUtils.IGNORE_ABOVE;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD;
import static com.linkedin.metadata.search.utils.ESUtils.KEYWORD_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.TEXT_FIELD_TYPE;
import static com.linkedin.metadata.search.utils.ESUtils.TYPE;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.models.annotation.SearchableAnnotation.FieldType;
import com.linkedin.metadata.models.annotation.v3.FieldMapping;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.utils.ESUtils;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The shared search fields of a Search V3 entity index. Full-text search reads root fields named by
 * {@code searchLabel}, not a {@code _search} object:
 *
 * <ul>
 *   <li>a searchable field copies into the shared field its label and degree name, {@code tags} or
 *       {@code tags_1}, or the field its {@code entityFieldName} names. A label is the semantic
 *       bucket. Degree 0 is the value on the entity, and a higher degree is a child or related
 *       copy. The entity name is {@code name}. A schema field path or label is that name at degree
 *       1 ({@code name_1});
 *   <li>a string field queried by default in V3 that names none copies into the root field {@code
 *       other}. A field with {@code v3.queryByDefault: false} is excluded even when V2 queries it
 *       by default. An entity whose fields name no {@code name} sends the field its {@code
 *       _entityName} alias names to {@code name} instead. The urn and fields of an entity a
 *       reference field names ({@code @SearchableRef}) only ever copy into {@code other}, and
 *       matched fields do not report them.
 * </ul>
 *
 * <p>A field of type {@code URN} or {@code URN_PARTIAL}, or a root mapping of type {@code URN},
 * keeps two sibling roots. {@code <field>} is the parsed name, a normalized keyword. {@code text}
 * and {@code stemmed} are added only when that field is included in full text. {@code <field>_urn}
 * is the complete case-preserving URN. Its only subfield is {@code <field>_urn.lowercase}, the same
 * URN through {@code keyword_normalizer}. Filters, facets, sorts, and ranges read {@code
 * <field>_urn}. A case-insensitive full-URN match reads {@code <field>_urn.lowercase}. A field with
 * {@code queryByDefault: false}, such as {@code owners}, keeps the parsed keyword and has neither
 * analyzer. The parsed name is also projected to {@code _aspects.<aspect>.<field>__urn_name}, which
 * copies into the parsed root. A URN that names a shared label copies both values onto that label.
 * The document URN stays an exact keyword. Its only subfield is {@code urn.lowercase}, the same URN
 * through {@code keyword_normalizer}. A case-insensitive match on the document reads that subfield.
 * The parsed identity is the root field {@code urn_name}, stored in its original case, with {@code
 * urn_name.lowercase} for a case-insensitive match. {@code urn_name} is not a full-text field. A
 * URN structured property keeps the exact URN on {@code structuredProperties.<field>}, with {@code
 * .lowercase}. The parsed name is {@code structuredProperties.<field>__urn_name}, in its original
 * case, and a case-insensitive match reads {@code __urn_name.lowercase}. With {@code
 * searchConfiguration} that sibling also has {@code text} and {@code stemmed}. {@code copyTo}
 * copies the parsed name and {@code copyUrnTo} copies the complete URN. Search reads the analyzed
 * subfields of a configured property. {@code queryByDefault} decides whether an unqualified query
 * includes them. The stored property value is unchanged.
 *
 * <p>These shared fields are the only full-text fields. Root fields and the {@code _aspects} copies
 * stay keywords and typed values for filters, facets and sorts, except the browse path v2 root,
 * which keeps its path analyzer. Browse path v1 is not indexed on Search V3. A shared field is a
 * full-text field when at least one string field queried by default feeds it in the index;
 * otherwise it stays a plain keyword, as the dates and other labels do.
 *
 * <p>Several root fields feeding one shared field are a union: every value is searchable, rebuilt
 * from the whole document on each write. When two aspects write the same root field, the value that
 * wins at the root (the user-edited aspect within a batch, the last write across batches) is the
 * one copied.
 */
public final class V3SearchFields {

  public static final String ENTITY_NAME = "name";
  public static final String QUALIFIED_NAME = "qualifiedName";
  public static final String DESCRIPTION = "description";
  public static final String OTHER = "other";

  /**
   * Suffix of the aspect keyword that stores a field's parsed URN name. The field is {@code
   * _aspects.<aspect>.<field>__urn_name}.
   */
  public static final String PARSED_URN_SUFFIX = "__urn_name";

  /**
   * Suffix of the root that stores a field's complete URN. The field is {@code <field>_urn}. The
   * parsed name occupies {@code <field>} itself.
   */
  public static final String FULL_URN_SUFFIX = "_urn";

  /** Case-folded multi-field of {@code <field>_urn}. It is not a text analyzer. */
  public static final String LOWERCASE = "lowercase";

  /**
   * Former object property that stored a parsed URN name. Parent stripping still removes a trailing
   * {@code .urnName} so an older path resolves to the parsed root.
   */
  public static final String PARSED_KEYWORD = "urnName";

  public static final String STRUCTURED_PROPERTIES = "structuredProperties";

  /** Word subfield of every full-text shared field. */
  public static final String TEXT = FieldMapping.TEXT;

  /** Indexed path suffix of {@link #TEXT}. */
  public static final String TEXT_SUFFIX = "." + TEXT;

  /** Stemmed subfield of every full-text shared field. */
  public static final String STEMMED = FieldMapping.STEMMED;

  /** Indexed path suffix of {@link #STEMMED}. */
  public static final String STEMMED_SUFFIX = "." + STEMMED;

  public static final String TEXT_ANALYZER = "v3_text";
  public static final String TEXT_SEARCH_ANALYZER = "v3_text_search";
  public static final String STEMMED_ANALYZER = "v3_stemmed";
  public static final String STEMMED_SEARCH_ANALYZER = "v3_stemmed_search";

  /** Word tokenizer of those analyzers, unless a main tokenizer is configured. */
  public static final String WORD_TOKENIZER = "v3_word_tokenizer";

  private static final String ENTITY_NAME_ALIAS = "_entityName";

  /**
   * Root fields that hold one value per schema field. Matched fields never fetch them: a wide table
   * would be returned whole with every hit. Their search labels still decide which shared field is
   * searched.
   */
  public static final Set<String> COLUMN_ARRAY_FIELDS =
      Set.of(
          "fieldPaths",
          "fieldLabels",
          "fieldDescriptions",
          "editedFieldDescriptions",
          "fieldTags",
          "editedFieldTags",
          "fieldGlossaryTerms",
          "editedFieldGlossaryTerms");

  // The fields of other that matched fields report: those that name or tag the entity. The rest of
  // other, such as a document's text, custom properties or lists of urns, has no size bound
  static final Set<String> MATCHED_OTHER_FIELDS =
      Set.of("name", "editedName", "displayName", "fullName", "title", "tags", "glossaryTerms");

  // Shared fields that name the entity. Query weights use {@link #WEIGHTS}; the mapping shape comes
  // from the label's resolved root, not from this set.
  private static final Set<String> IDENTITY_FIELDS = Set.of(ENTITY_NAME, QUALIFIED_NAME);

  // Built-in destinations with no PDL field. Their keyword root is not an exact field; full text
  // reads the analyzed subfields. Model labels do not use this set. Structured properties are not
  // a shared field.
  private static final Set<String> SEARCH_ONLY_BUILTINS = Set.of(OTHER);

  // Indexed shared keywords skip longer values, whose UTF-8 bytes could exceed the engine's term
  // limit: copy_to copies a value the source field skipped
  private static final int KEYWORD_IGNORE_ABOVE =
      ESUtils.keywordIgnoreAboveForMaxBytes(ESUtils.KEYWORD_MAXLENGTH);

  /**
   * The Search V3 hasValues boolean for the indexed field {@code name}. Degree 0 keeps the declared
   * name. Degree n appends {@code _n}. Absent when none declare one.
   */
  @Nonnull
  public static Optional<String> hasValuesFieldName(
      @Nonnull EntitySpec entitySpec, @Nonnull String name) {
    return entitySpec.getAspectSpecs().stream()
        .flatMap(aspect -> aspect.getSearchableFieldSpecs().stream())
        .map(SearchableFieldSpec::getSearchableAnnotation)
        .map(SearchableAnnotation::resolvedForV3)
        .filter(SearchableAnnotation::isV3Searchable)
        .filter(annotation -> name.equals(annotation.indexedFieldName().orElse(null)))
        .map(SearchableAnnotation::indexedHasValuesFieldName)
        .flatMap(Optional::stream)
        .findFirst();
  }

  // Relative weight of built-in fields that are not a PDL label. A PDL label's weight is the
  // resolved boost of its indexed field.
  private static final Map<String, Float> WEIGHTS = Map.of(OTHER, 0.5f);

  private static final Set<FieldType> STRING_FIELD_TYPES =
      Set.of(
          FieldType.KEYWORD,
          FieldType.TEXT,
          FieldType.TEXT_PARTIAL,
          FieldType.WORD_GRAM,
          FieldType.URN,
          FieldType.URN_PARTIAL);

  private V3SearchFields() {}

  /** Where a shared field is indexed and queried. Labels are root fields. */
  @Nonnull
  public static String path(@Nonnull final String name) {
    return name;
  }

  public static boolean isIdentity(@Nonnull final String name) {
    return IDENTITY_FIELDS.contains(name);
  }

  public static float weight(@Nonnull final String name) {
    return WEIGHTS.getOrDefault(name, 1.0f);
  }

  /** Aspect field that stores {@code fieldName}'s parsed URN name. */
  @Nonnull
  public static String parsedUrnField(@Nonnull final String fieldName) {
    return fieldName + PARSED_URN_SUFFIX;
  }

  /** Parsed-name keyword. It is the logical field itself, {@code <field>}. */
  @Nonnull
  public static String parsedKeywordField(@Nonnull final String label) {
    return label;
  }

  /** Complete URN sibling of a logical field, {@code <field>_urn}. */
  @Nonnull
  public static String fullUrnField(@Nonnull final String label) {
    return label + FULL_URN_SUFFIX;
  }

  /** Case-folded complete URN, {@code <field>_urn.lowercase}. */
  @Nonnull
  public static String fullUrnLowercaseField(@Nonnull final String label) {
    return fullUrnField(label) + "." + LOWERCASE;
  }

  /**
   * A searchable URN, including one excluded from full text. Its parsed name is indexed on the
   * aspect.
   */
  public static boolean contributesParsedUrn(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation().resolvedForV3();
    return annotation.isV3Searchable() && isExactUrn(fieldSpec);
  }

  /** An exact URN included in full text. Its parsed name is what full text reads. */
  public static boolean contributesFullTextUrn(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation().resolvedForV3();
    return contributesParsedUrn(fieldSpec) && annotation.isQueryByDefault();
  }

  /**
   * Labels whose root value is the full URN and whose parsed name is the label keyword. A label
   * qualifies only when every field that names it is an exact URN, so a shared text label such as
   * {@code name} stays a keyword. {@code queryByDefault: false} still qualifies: {@code owners}
   * stores the parsed keyword and does not gain analyzers.
   */
  @Nonnull
  public static Set<String> parsedKeywordLabels(@Nonnull final Collection<EntitySpec> entitySpecs) {
    return new LinkedHashSet<>(UrnRoots.of(entitySpecs).parsedKeywordLabels());
  }

  public static boolean isParsedKeywordLabel(
      @Nonnull final String label, @Nonnull final Collection<EntitySpec> entitySpecs) {
    return UrnRoots.of(entitySpecs).parsedKeywordLabels().contains(label);
  }

  /**
   * The projected root named {@code fieldName} stores the parsed URN name. A field that only feeds
   * a different URN label, such as {@code fieldTags} feeding {@code tags}, stays the raw keyword;
   * the label is the parsed root and {@code tags_urn} holds the complete URN.
   */
  public static boolean isUrnObjectField(
      @Nonnull final String fieldName, @Nonnull final Collection<EntitySpec> entitySpecs) {
    return UrnRoots.of(entitySpecs).isUrnObjectField(fieldName);
  }

  /** A logical field whose value is the parsed URN name, with the complete URN on {@code _urn}. */
  public static boolean isParsedUrnRoot(
      @Nonnull final String name, @Nonnull final Collection<EntitySpec> entitySpecs) {
    return UrnRoots.of(entitySpecs).isParsedUrnRoot(name);
  }

  /**
   * {@code text} and {@code stemmed} belong on the parsed root only when every contributor is
   * included in full text. {@code owners} keeps the parsed keyword and has neither analyzer.
   */
  public static boolean urnRootHasAnalyzers(
      @Nonnull final String name, @Nonnull final Collection<EntitySpec> entitySpecs) {
    return UrnRoots.of(entitySpecs).hasAnalyzers(name);
  }

  /** URN roots whose parsed name full text reads. */
  @Nonnull
  private static Set<String> analyzedUrnRoots(@Nonnull final Collection<EntitySpec> entitySpecs) {
    return UrnRoots.of(entitySpecs).analyzedRoots();
  }

  /** Logical fields that store a parsed URN name. */
  @Nonnull
  private static Set<String> parsedUrnRoots(@Nonnull final Collection<EntitySpec> entitySpecs) {
    return new LinkedHashSet<>(UrnRoots.of(entitySpecs).parsedRoots());
  }

  /**
   * The parsed-URN facts of one entity list. The registry's specs do not change while GMS runs, and
   * the projector asks for them for every field of every document, so they are built once per list
   * of spec instances. {@link EntitySpec} has identity equality, so a different spec with the same
   * name is a different key.
   */
  private static final Cache<List<EntitySpec>, UrnRoots> URN_ROOTS =
      Caffeine.newBuilder().maximumSize(256).build();

  private record UrnRoots(
      Set<String> parsedKeywordLabels,
      Map<String, Boolean> labelFullText,
      Map<String, Boolean> objectFields,
      Map<String, Boolean> objectFullText,
      Set<String> parsedRoots) {

    @Nonnull
    static UrnRoots of(@Nonnull final Collection<EntitySpec> entitySpecs) {
      return URN_ROOTS.get(List.copyOf(entitySpecs), UrnRoots::build);
    }

    @Nonnull
    private static UrnRoots build(@Nonnull final Collection<EntitySpec> entitySpecs) {
      final Map<String, Boolean> urnOnly = new LinkedHashMap<>();
      final Map<String, Boolean> labelFullText = new HashMap<>();
      forEachFieldSpec(
          entitySpecs,
          (aspectSpec, fieldSpec) -> {
            if (!fieldSpec.getSearchableAnnotation().isV3Searchable()) {
              return;
            }
            final boolean parsedUrn = contributesParsedUrn(fieldSpec);
            final boolean fullText = contributesFullTextUrn(fieldSpec);
            for (String label : labels(fieldSpec)) {
              urnOnly.merge(label, parsedUrn, Boolean::logicalAnd);
              labelFullText.merge(label, fullText, Boolean::logicalAnd);
            }
          });
      final Set<String> parsedKeywordLabels = new LinkedHashSet<>();
      urnOnly.forEach(
          (label, onlyParsedUrns) -> {
            if (onlyParsedUrns) {
              parsedKeywordLabels.add(label);
            }
          });

      final Map<String, Boolean> objectFields = new HashMap<>();
      final Map<String, Boolean> objectFullText = new HashMap<>();
      final Set<String> parsedRoots = new LinkedHashSet<>(parsedKeywordLabels);
      forEachFieldSpec(
          entitySpecs,
          (aspectSpec, fieldSpec) -> {
            final SearchableAnnotation annotation =
                fieldSpec.getSearchableAnnotation().resolvedForV3();
            if (!annotation.isV3Searchable()) {
              return;
            }
            final String fieldName = annotation.getFieldName();
            final boolean exact =
                isExactUrn(fieldSpec)
                    && labels(fieldSpec).stream()
                        .noneMatch(
                            label ->
                                parsedKeywordLabels.contains(label) && !label.equals(fieldName));
            objectFields.merge(fieldName, exact, Boolean::logicalAnd);
            objectFullText.merge(fieldName, contributesFullTextUrn(fieldSpec), Boolean::logicalAnd);
          });
      // Every field is visited, searchable or not, so the roots keep their declaration order
      forEachFieldSpec(
          entitySpecs,
          (aspectSpec, fieldSpec) -> {
            final String fieldName =
                fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
            if (Boolean.TRUE.equals(objectFields.get(fieldName))) {
              parsedRoots.add(fieldName);
            }
          });
      return new UrnRoots(
          Collections.unmodifiableSet(parsedKeywordLabels),
          Map.copyOf(labelFullText),
          Map.copyOf(objectFields),
          Map.copyOf(objectFullText),
          Collections.unmodifiableSet(parsedRoots));
    }

    boolean isUrnObjectField(@Nonnull final String fieldName) {
      return Boolean.TRUE.equals(objectFields.get(fieldName));
    }

    boolean isParsedUrnRoot(@Nonnull final String name) {
      return parsedKeywordLabels.contains(name) || isUrnObjectField(name);
    }

    boolean parsedKeywordHasAnalyzers(@Nonnull final String label) {
      return parsedKeywordLabels.contains(label) && Boolean.TRUE.equals(labelFullText.get(label));
    }

    boolean hasAnalyzers(@Nonnull final String name) {
      if (parsedKeywordLabels.contains(name)) {
        return parsedKeywordHasAnalyzers(name);
      }
      return isUrnObjectField(name) && Boolean.TRUE.equals(objectFullText.get(name));
    }

    @Nonnull
    Set<String> analyzedRoots() {
      final Set<String> names = new LinkedHashSet<>(parsedRoots);
      names.removeIf(name -> !hasAnalyzers(name));
      return names;
    }
  }

  private static void forEachFieldSpec(
      @Nonnull final Collection<EntitySpec> entitySpecs,
      @Nonnull final BiConsumer<AspectSpec, SearchableFieldSpec> consumer) {
    for (EntitySpec entitySpec : entitySpecs) {
      if (entitySpec.getAspectSpecs() == null) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          consumer.accept(aspectSpec, fieldSpec);
        }
      }
    }
  }

  /**
   * Reject a full-URN sibling whose name is already a searchable field or label in the index group.
   */
  public static void validateFullUrnSiblings(@Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<String> occupied = new LinkedHashSet<>();
    for (EntitySpec entitySpec : entitySpecs) {
      if (entitySpec.getAspectSpecs() == null) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          if (!fieldSpec.getSearchableAnnotation().isV3Searchable()) {
            continue;
          }
          final SearchableAnnotation annotation =
              fieldSpec.getSearchableAnnotation().resolvedForV3();
          occupied.add(annotation.getFieldName());
          occupied.addAll(labels(fieldSpec));
        }
      }
    }
    if (occupied.contains(MappingConstants.URN_NAME_FIELD)) {
      throw new IllegalArgumentException(
          "Search V3 reserves the root field urn_name for the document URN's parsed identity");
    }
    for (String name : parsedUrnRoots(entitySpecs)) {
      final String sibling = fullUrnField(name);
      if (occupied.contains(sibling)) {
        throw new IllegalArgumentException(
            "Search V3 field '"
                + sibling
                + "' collides with the full URN sibling of '"
                + name
                + "'");
      }
    }
  }

  /**
   * {@code text} and {@code stemmed} belong on a URN label only when every contributor is included
   * in full text.
   */
  public static boolean parsedKeywordHasAnalyzers(
      @Nonnull final String label, @Nonnull final Collection<EntitySpec> entitySpecs) {
    return UrnRoots.of(entitySpecs).parsedKeywordHasAnalyzers(label);
  }

  /**
   * Where the parsed name is copied. A URN root receives the logical field whether or not it is
   * included in full text. A shared URN label receives the same name. A full-text URN with no
   * parsed root copies the name to {@code other}.
   */
  @Nonnull
  public static List<String> parsedNameDestinations(
      @Nonnull final SearchableFieldSpec fieldSpec,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return parsedNameDestinations(fieldSpec, UrnRoots.of(entitySpecs));
  }

  @Nonnull
  private static List<String> parsedNameDestinations(
      @Nonnull final SearchableFieldSpec fieldSpec, @Nonnull final UrnRoots roots) {
    if (!contributesParsedUrn(fieldSpec)) {
      return List.of();
    }
    final String fieldName = fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
    final Set<String> declared = labels(fieldSpec);
    final List<String> destinations = new ArrayList<>();
    if (roots.isParsedUrnRoot(fieldName)) {
      destinations.add(parsedKeywordField(fieldName));
    }
    for (String label : declared) {
      if (label.equals(fieldName)) {
        continue;
      }
      if (roots.parsedKeywordLabels().contains(label)) {
        destinations.add(parsedKeywordField(label));
      } else if (contributesFullTextUrn(fieldSpec)) {
        destinations.add(label);
      }
    }
    if (destinations.isEmpty() && contributesFullTextUrn(fieldSpec)) {
      destinations.add(OTHER);
    }
    return List.copyOf(destinations);
  }

  /**
   * {@code copy_to} targets that aggregate complete URNs onto {@code <field>_urn}. The logical root
   * is cleared because it stores the parsed name.
   */
  @Nonnull
  public static List<String> fullUrnCopyTargets(
      @Nonnull final SearchableFieldSpec fieldSpec,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final String fieldName = fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
    final UrnRoots roots = UrnRoots.of(entitySpecs);
    final Set<String> targets = new LinkedHashSet<>();
    if (isExactUrn(fieldSpec) && roots.isParsedUrnRoot(fieldName)) {
      targets.add(fullUrnField(fieldName));
    }
    if (contributesParsedUrn(fieldSpec)) {
      for (String label : labels(fieldSpec)) {
        if (roots.parsedKeywordLabels().contains(label)) {
          targets.add(fullUrnField(label));
        }
      }
    }
    return List.copyOf(targets);
  }

  /**
   * The logical root stores the parsed name, so the projector clears the complete URN there. The
   * complete URN is copied from the aspect field onto {@code <field>_urn}.
   */
  public static boolean clearsRootForUrnLabel(
      @Nonnull final SearchableFieldSpec fieldSpec,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final String fieldName = fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
    return isExactUrn(fieldSpec) && isParsedUrnRoot(fieldName, entitySpecs);
  }

  /**
   * Parsed URN name. A normalized keyword, with {@code text} and {@code stemmed} only when the
   * field is included in full text.
   */
  @Nonnull
  public static Map<String, Object> parsedUrnRootMapping(final boolean withAnalyzers) {
    final Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    if (withAnalyzers) {
      mapping.put(FIELDS, analyzedSubfields());
    }
    return mapping;
  }

  /**
   * Complete URN. Case-preserving keyword with a {@code lowercase} multi-field and no text
   * analyzers.
   */
  @Nonnull
  public static Map<String, Object> fullUrnRootMapping() {
    final Map<String, Object> mapping = new HashMap<>(FieldTypeMapper.getMappingsForUrn());
    mapping.put(FIELDS, Map.of(LOWERCASE, lowercaseKeywordField()));
    return mapping;
  }

  /** Case-folded keyword multi-field. Lowercase and ASCII fold, not a text analyzer. */
  @Nonnull
  public static Map<String, Object> lowercaseKeywordField() {
    final Map<String, Object> lowercase = new HashMap<>();
    lowercase.put(TYPE, KEYWORD_FIELD_TYPE);
    lowercase.put(NORMALIZER, KEYWORD_NORMALIZER);
    return lowercase;
  }

  /**
   * Aspect keywords whose analyzers are the full-text fields, because the parsed name is not copied
   * onto a root label.
   */
  @Nonnull
  public static List<String> directParsedFullTextFields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final UrnRoots roots = UrnRoots.of(entitySpecs);
    final List<String> paths = new ArrayList<>();
    for (EntitySpec entitySpec : entitySpecs) {
      if (entitySpec.getAspectSpecs() == null) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          if (!contributesFullTextUrn(fieldSpec)
              || !parsedNameDestinations(fieldSpec, roots).isEmpty()) {
            continue;
          }
          final String fieldName =
              fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
          paths.add(aspectParsedName(aspectSpec.getName(), fieldName));
        }
      }
    }
    return paths;
  }

  /**
   * Aspect fields that store the parsed name of an analyzed URN label. The root is null in {@code
   * _source}, so matched fields read these paths and report the label.
   */
  @Nonnull
  public static Map<String, List<String>> storedParsedNameFields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final UrnRoots roots = UrnRoots.of(entitySpecs);
    final Set<String> labels = roots.analyzedRoots();
    final Map<String, List<String>> stored = new LinkedHashMap<>();
    for (EntitySpec entitySpec : entitySpecs) {
      if (entitySpec.getAspectSpecs() == null) {
        continue;
      }
      for (AspectSpec aspectSpec : entitySpec.getAspectSpecs()) {
        for (SearchableFieldSpec fieldSpec : aspectSpec.getSearchableFieldSpecs()) {
          if (!contributesParsedUrn(fieldSpec)) {
            continue;
          }
          final String fieldName =
              fieldSpec.getSearchableAnnotation().resolvedForV3().getFieldName();
          final String path = aspectParsedName(aspectSpec.getName(), fieldName);
          for (String destination : parsedNameDestinations(fieldSpec, roots)) {
            if (!labels.contains(destination)) {
              continue;
            }
            final List<String> paths =
                stored.computeIfAbsent(destination, ignored -> new ArrayList<>());
            if (!paths.contains(path)) {
              paths.add(path);
            }
          }
        }
      }
    }
    return stored;
  }

  @Nonnull
  private static String aspectParsedName(
      @Nonnull final String aspectName, @Nonnull final String fieldName) {
    return MappingConstants.ASPECTS_FIELD_NAME + "." + aspectName + "." + parsedUrnField(fieldName);
  }

  /**
   * Label mappings. A URN keyword stores the full URN, so it does not gain {@code text} or {@code
   * stemmed}; those analyzers live on the parsed keyword.
   */
  @Nonnull
  public static List<FieldMapping> labelMappings(@Nonnull final SearchableFieldSpec fieldSpec) {
    final List<FieldMapping> mappings =
        new ArrayList<>(FieldMapping.of(fieldSpec.getSearchableAnnotation()));
    if (isExactUrn(fieldSpec)) {
      mappings.removeIf(
          mapping -> TEXT.equals(mapping.getSubfield()) || STEMMED.equals(mapping.getSubfield()));
    }
    return mappings;
  }

  /**
   * Case-insensitive keyword for a parsed URN name. No {@code ignore_above}: a dataset name or
   * field path can exceed the usual keyword limit. {@code copyTo} is omitted when empty.
   */
  @Nonnull
  public static Map<String, Object> parsedUrnKeywordMapping(@Nonnull final List<String> copyTo) {
    return parsedUrnKeywordMapping(copyTo, false);
  }

  /**
   * Indexed parsed-name keyword. {@code withAnalyzers} adds {@code text} and {@code stemmed} when
   * full text reads this field directly.
   */
  @Nonnull
  public static Map<String, Object> parsedUrnKeywordMapping(
      @Nonnull final List<String> copyTo, final boolean withAnalyzers) {
    final Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, KEYWORD_FIELD_TYPE);
    mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
    if (!copyTo.isEmpty()) {
      mapping.put(ESUtils.COPY_TO, List.copyOf(copyTo));
    }
    if (withAnalyzers) {
      mapping.put(FIELDS, analyzedSubfields());
    }
    return mapping;
  }

  /**
   * A raw URN stays exact. Its parsed name is a separate field. Full text receives that name only
   * when the field is included in full text.
   */
  private static boolean isExactUrn(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation().resolvedForV3();
    final FieldType fieldType = annotation.getFieldType();
    if (fieldType == FieldType.URN || fieldType == FieldType.URN_PARTIAL) {
      return true;
    }
    return FieldMapping.of(annotation).stream()
        .anyMatch(mapping -> mapping.isRoot() && mapping.getType() == FieldMapping.Type.URN);
  }

  /**
   * The default query weight of an indexed field. A PDL label uses the resolved boost its sources
   * agreed on. {@code other} stays on {@link #WEIGHTS}.
   */
  public static float weight(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String name) {
    if (WEIGHTS.containsKey(name)) {
      return WEIGHTS.get(name);
    }
    OptionalDouble resolved =
        entitySpecs.stream()
            .flatMap(entitySpec -> entitySpec.getAspectSpecs().stream())
            .flatMap(aspectSpec -> aspectSpec.getSearchableFieldSpecs().stream())
            .map(SearchableFieldSpec::getSearchableAnnotation)
            .map(SearchableAnnotation::resolvedForV3)
            .filter(SearchableAnnotation::isV3Searchable)
            .filter(annotation -> name.equals(annotation.indexedFieldName().orElse(null)))
            .mapToDouble(SearchableAnnotation::getBoostScore)
            .findFirst();
    return resolved.isPresent() ? (float) resolved.getAsDouble() : WEIGHTS.getOrDefault(name, 1.0f);
  }

  /** Whether a field of this type holds strings, the only values the shared fields copy. */
  public static boolean isStringFieldType(@Nonnull final FieldType fieldType) {
    return STRING_FIELD_TYPES.contains(fieldType);
  }

  /**
   * The shared fields the root field of these source fields copies into. A label on any source wins
   * over the fallback to {@code other}, so a root field never lands in both.
   */
  @Nonnull
  public static Set<String> destinations(
      @Nonnull final Collection<SearchableFieldSpec> sourceFieldSpecs,
      @Nonnull final Set<SearchableFieldSpec> entityNameFallbacks) {
    final Set<String> destinations = new LinkedHashSet<>();
    sourceFieldSpecs.forEach(fieldSpec -> destinations.addAll(labels(fieldSpec)));
    if (destinations.isEmpty()) {
      sourceFieldSpecs.stream()
          .filter(V3SearchFields::isQueriedByDefault)
          .filter(fieldSpec -> !isExactUrn(fieldSpec))
          .map(fieldSpec -> entityNameFallbacks.contains(fieldSpec) ? ENTITY_NAME : OTHER)
          .forEach(destinations::add);
    }
    return destinations;
  }

  /**
   * For entities none of whose fields name {@code name}, the fields their {@code _entityName} alias
   * names. They feed {@code name} instead of {@code other}, so every entity's name is in the same
   * shared field.
   */
  @Nonnull
  public static Set<SearchableFieldSpec> entityNameFallbacks(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .filter(
            entitySpec ->
                sourceFieldSpecs(entitySpec).noneMatch(spec -> labels(spec).contains(ENTITY_NAME)))
        .flatMap(V3SearchFields::sourceFieldSpecs)
        .filter(
            spec -> {
              final List<String> aliases = spec.getSearchableAnnotation().getFieldNameAliases();
              return aliases != null && aliases.contains(ENTITY_NAME_ALIAS);
            })
        .collect(Collectors.toSet());
  }

  /**
   * The shared fields full-text search reads for these entities, each with the root fields that
   * feed it, in the order the entities and their aspects declare them. A label is listed when one
   * of its own contributors declares a {@code text} subfield and is queried by default. Another
   * degree of the same source field does not opt it in. {@code other} is listed for annotated URN
   * identities and the remaining default text. Structured properties are not listed: search reads
   * the root field of each opted-in property.
   */
  @Nonnull
  public static Map<String, List<String>> fullTextFields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    final Map<String, Set<String>> sources = new LinkedHashMap<>();
    final Set<String> queried = new LinkedHashSet<>();
    rootSourceFieldSpecs(entitySpecs)
        .forEach(
            (rootField, fieldSpecs) -> {
              final boolean groupHasLabel =
                  fieldSpecs.stream().anyMatch(spec -> !labels(spec).isEmpty());
              for (String destination : destinations(fieldSpecs, fallbacks)) {
                sources.computeIfAbsent(destination, k -> new LinkedHashSet<>()).add(rootField);
                if (fieldSpecs.stream()
                    .anyMatch(
                        spec ->
                            landsIn(spec, destination, fallbacks, groupHasLabel)
                                && declaresText(spec)
                                && isQueriedByDefault(spec))) {
                  queried.add(destination);
                }
              }
            });
    // The catch-all goes last, so a hit's matched fields list the specific fields first.
    // Analyzed URN labels are specific fields, ahead of other.
    final Set<String> analyzedLabels = analyzedUrnRoots(entitySpecs);
    final Map<String, List<SearchableFieldSpec>> roots = rootSourceFieldSpecs(entitySpecs);
    queried.remove(OTHER);
    queried.addAll(analyzedLabels);
    queried.add(OTHER);
    final Map<String, List<String>> fields = new LinkedHashMap<>();
    for (String name : queried) {
      if (analyzedLabels.contains(name)) {
        // An array of URNs is searched, but matched fields do not fetch it whole. Tags and
        // glossary terms stay, as the short lists matched fields already show.
        final boolean fetch =
            roots.getOrDefault(name, List.of()).stream().noneMatch(SearchableFieldSpec::isArray)
                || "tags".equals(name)
                || "glossaryTerms".equals(name);
        fields.put(name, fetch ? List.of(name) : List.of());
        continue;
      }
      fields.put(name, List.copyOf(sources.getOrDefault(name, Set.of())));
    }
    for (String path : directParsedFullTextFields(entitySpecs)) {
      fields.put(path, List.of(path));
    }
    return fields;
  }

  /**
   * The root fields of the shared fields a search reads whose values matched fields are found from:
   * not {@code name_1} or the schema-field arrays, and of {@code other} only the fields that name
   * or tag the entity, which "Matched on" shows. A fetch cannot cut a value short, so a table with
   * thousands of fields, a long document or a long list of urns would be sent whole with every hit;
   * search still reads every shared field.
   */
  @Nonnull
  public static List<String> matchedFieldSources(
      @Nonnull final Map<String, List<String>> searchedFields) {
    return searchedFields.entrySet().stream()
        .filter(
            field -> !SearchableAnnotation.indexedFieldName(ENTITY_NAME, 1).equals(field.getKey()))
        .flatMap(
            field ->
                OTHER.equals(field.getKey())
                    ? field.getValue().stream().filter(MATCHED_OTHER_FIELDS::contains)
                    : field.getValue().stream())
        .filter(source -> !COLUMN_ARRAY_FIELDS.contains(source))
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * The source fields of each root field, by root field name: a root field holds every searchable
   * field of that name, whichever aspect declares it. The mappings builder copies each root field
   * into the shared fields of all of them, so the mapping and the query agree.
   */
  @Nonnull
  public static Map<String, List<SearchableFieldSpec>> rootSourceFieldSpecs(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    return entitySpecs.stream()
        .flatMap(V3SearchFields::sourceFieldSpecs)
        .collect(
            Collectors.groupingBy(
                spec -> spec.getSearchableAnnotation().getFieldName(),
                LinkedHashMap::new,
                Collectors.toList()));
  }

  /**
   * These entities and every entity that shares a V3 index with one of them ({@code searchGroup}).
   * The mapping routes a root field by every entity of its index, so a label one entity puts on a
   * field also routes the fields of that name of the others; a search reads what the mapping wrote.
   */
  @Nonnull
  public static List<EntitySpec> indexGroupSpecs(
      @Nonnull final EntityRegistry entityRegistry,
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<EntitySpec> groups = new LinkedHashSet<>();
    for (EntitySpec entitySpec : entitySpecs) {
      final Collection<EntitySpec> group =
          V3IndexKeys.entitySpecsForKey(entityRegistry, V3IndexKeys.resolve(entitySpec));
      groups.addAll(group.isEmpty() ? List.of(entitySpec) : group);
    }
    return List.copyOf(groups);
  }

  /**
   * Shared fields a structured property's {@code copyTo} writes into for these entities. An entry
   * names a shared field ({@code entityName}) or a root field that feeds one ({@code name}). A name
   * that is neither is skipped. Only a field full-text search reads is kept.
   */
  @Nonnull
  public static List<String> copyDestinations(
      @Nonnull final Collection<EntitySpec> entitySpecs,
      @Nullable final Collection<String> configured) {
    if (configured == null || configured.isEmpty()) {
      return List.of();
    }
    return copyDestinations(
        fullTextFields(entitySpecs).keySet(),
        rootSourceFieldSpecs(entitySpecs),
        entityNameFallbacks(entitySpecs),
        configured);
  }

  @Nonnull
  static List<String> copyDestinations(
      @Nonnull final Set<String> fullText,
      @Nonnull final Map<String, List<SearchableFieldSpec>> roots,
      @Nonnull final Set<SearchableFieldSpec> fallbacks,
      @Nullable final Collection<String> configured) {
    if (configured == null || configured.isEmpty()) {
      return List.of();
    }
    final Set<String> destinations = new LinkedHashSet<>();
    for (String entry : configured) {
      if (entry == null || entry.isBlank()) {
        continue;
      }
      final String name =
          entry.startsWith("_search.") ? entry.substring("_search.".length()) : entry;
      if (fullText.contains(name)) {
        destinations.add(name);
        continue;
      }
      final List<SearchableFieldSpec> sources = roots.get(name);
      if (sources == null) {
        continue;
      }
      destinations(sources, fallbacks).stream()
          .filter(fullText::contains)
          .forEach(destinations::add);
    }
    return List.copyOf(destinations);
  }

  /** Whether a root field of these entities copies into the shared field {@code name}. */
  public static boolean isFed(
      @Nonnull final Collection<EntitySpec> entitySpecs, @Nonnull final String name) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    return rootSourceFieldSpecs(entitySpecs).values().stream()
        .anyMatch(sources -> destinations(sources, fallbacks).contains(name));
  }

  /**
   * Whether the target mapping has an analyzed shared field, one with a {@code text} subfield, that
   * the current mapping lacks. Full-text search reads that subfield, so an index without it quietly
   * matches nothing.
   */
  public static boolean lacksSharedSearchFields(
      @Nonnull final Map<String, Object> currentMappings,
      @Nonnull final Map<String, Object> targetMappings) {
    final Map<?, ?> current = rootProperties(currentMappings);
    return rootProperties(targetMappings).entrySet().stream()
        .anyMatch(
            field -> isAnalyzed(field.getValue()) && !isAnalyzed(current.get(field.getKey())));
  }

  @Nonnull
  private static Map<?, ?> rootProperties(@Nonnull final Map<String, Object> mappings) {
    return mappings.get("properties") instanceof Map<?, ?> properties ? properties : Map.of();
  }

  private static boolean isAnalyzed(final Object mapping) {
    return mapping instanceof Map<?, ?> fieldMapping
        && fieldMapping.get(FIELDS) instanceof Map<?, ?> subfields
        && subfields.containsKey(TEXT);
  }

  /**
   * The mapping of a built-in shared field. {@code other} and {@code structuredProperties} are a
   * search-only keyword plus {@code text} and {@code stemmed}. Model labels are composed from
   * {@code v3.mappings} and do not use this method.
   */
  @Nonnull
  public static Map<String, Object> mapping(@Nonnull final String name, final boolean fullText) {
    final Map<String, Object> mapping = new HashMap<>();
    mapping.put(TYPE, KEYWORD_FIELD_TYPE);
    if (!fullText || !SEARCH_ONLY_BUILTINS.contains(name)) {
      mapping.put(NORMALIZER, KEYWORD_NORMALIZER);
      mapping.put(IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE);
    } else {
      mapping.put(ESUtils.INDEX, false);
      mapping.put(DOC_VALUES, false);
    }
    if (!fullText) {
      return mapping;
    }
    final Map<String, Object> subfields = new HashMap<>();
    subfields.put(TEXT, textMapping(TEXT_ANALYZER, TEXT_SEARCH_ANALYZER));
    subfields.put(STEMMED, textMapping(STEMMED_ANALYZER, STEMMED_SEARCH_ANALYZER));
    if (!SEARCH_ONLY_BUILTINS.contains(name)) {
      subfields.put(KEYWORD, Map.of(TYPE, KEYWORD_FIELD_TYPE, IGNORE_ABOVE, KEYWORD_IGNORE_ABOVE));
    }
    mapping.put(FIELDS, subfields);
    return mapping;
  }

  /**
   * {@code text} and {@code stemmed} subfields for a keyword field full-text search reads, using
   * the V3 analyzers. The caller copies any existing subfields first: {@link FieldTypeMapper}'s
   * string mapping is an immutable map.
   */
  @Nonnull
  public static Map<String, Object> analyzedSubfields() {
    return Map.of(
        TEXT, textMapping(TEXT_ANALYZER, TEXT_SEARCH_ANALYZER),
        STEMMED, textMapping(STEMMED_ANALYZER, STEMMED_SEARCH_ANALYZER));
  }

  /**
   * Analyzer subfields declared for each shared field. Full-text search reads {@code text} and
   * {@code stemmed} only when the resolved label declares them. {@code other} always declares both.
   */
  @Nonnull
  public static Map<String, Set<String>> analyzerSubfields(
      @Nonnull final Collection<EntitySpec> entitySpecs) {
    final Set<SearchableFieldSpec> fallbacks = entityNameFallbacks(entitySpecs);
    final Map<String, Set<String>> subfields = new LinkedHashMap<>();
    rootSourceFieldSpecs(entitySpecs)
        .forEach(
            (rootField, fieldSpecs) -> {
              final Set<String> declared = labelSubfields(fieldSpecs);
              if (declared.isEmpty()) {
                return;
              }
              for (String destination : destinations(fieldSpecs, fallbacks)) {
                subfields
                    .computeIfAbsent(destination, key -> new LinkedHashSet<>())
                    .addAll(declared);
              }
            });
    subfields.put(OTHER, Set.of(TEXT, STEMMED));
    for (String label : analyzedUrnRoots(entitySpecs)) {
      subfields.put(label, Set.of(TEXT, STEMMED));
    }
    for (String path : directParsedFullTextFields(entitySpecs)) {
      subfields.put(path, Set.of(TEXT, STEMMED));
    }
    return subfields;
  }

  private static boolean declaresText(@Nonnull final Collection<SearchableFieldSpec> fieldSpecs) {
    return labelSubfields(fieldSpecs).contains(TEXT);
  }

  private static boolean declaresText(@Nonnull final SearchableFieldSpec fieldSpec) {
    return declaresText(List.of(fieldSpec));
  }

  /**
   * Whether this source copies into {@code destination}. A label on any source of the root field
   * wins over the fallback to {@code other}, matching {@link #destinations}.
   */
  private static boolean landsIn(
      @Nonnull final SearchableFieldSpec fieldSpec,
      @Nonnull final String destination,
      @Nonnull final Set<SearchableFieldSpec> entityNameFallbacks,
      final boolean groupHasLabel) {
    final Set<String> specLabels = labels(fieldSpec);
    if (!specLabels.isEmpty()) {
      return specLabels.contains(destination);
    }
    if (groupHasLabel) {
      return false;
    }
    return destination.equals(entityNameFallbacks.contains(fieldSpec) ? ENTITY_NAME : OTHER);
  }

  @Nonnull
  private static Set<String> labelSubfields(
      @Nonnull final Collection<SearchableFieldSpec> fieldSpecs) {
    return fieldSpecs.stream()
        .flatMap(spec -> labelMappings(spec).stream())
        .filter(mapping -> !mapping.isRoot())
        .map(FieldMapping::getSubfield)
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static Map<String, Object> textMapping(
      @Nonnull final String analyzer, @Nonnull final String searchAnalyzer) {
    return Map.of(TYPE, TEXT_FIELD_TYPE, ANALYZER, analyzer, SEARCH_ANALYZER, searchAnalyzer);
  }

  private static Set<String> labels(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation();
    if (!annotation.isV3Searchable()) {
      return Set.of();
    }
    final SearchableAnnotation resolved = annotation.resolvedForV3();
    final Set<String> destinations = new LinkedHashSet<>();
    resolved.indexedFieldName().ifPresent(destinations::add);
    resolved.getEntityFieldName().filter(name -> !name.isEmpty()).ifPresent(destinations::add);
    return destinations;
  }

  private static boolean isQueriedByDefault(@Nonnull final SearchableFieldSpec fieldSpec) {
    final SearchableAnnotation annotation = fieldSpec.getSearchableAnnotation().resolvedForV3();
    return annotation.isV3Searchable()
        && annotation.isQueryByDefault()
        && isStringFieldType(annotation.getFieldType());
  }

  /**
   * Searchable fields projected to the root: object fields have no root field, and structured
   * properties keep their own fields.
   */
  private static Stream<SearchableFieldSpec> sourceFieldSpecs(
      @Nonnull final EntitySpec entitySpec) {
    return entitySpec.getAspectSpecs().stream()
        .filter(aspectSpec -> !STRUCTURED_PROPERTIES_ASPECT_NAME.equals(aspectSpec.getName()))
        .map(AspectSpec::getSearchableFieldSpecs)
        .flatMap(List::stream)
        .filter(
            spec ->
                spec.getSearchableAnnotation().isV3Searchable()
                    && !OBJECT_FIELD_TYPES.contains(spec.getSearchableAnnotation().getFieldType()));
  }
}
