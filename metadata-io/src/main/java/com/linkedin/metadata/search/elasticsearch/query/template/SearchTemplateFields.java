package com.linkedin.metadata.search.elasticsearch.query.template;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.SearchableFieldSpec;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.search.template.HighlightFieldOverrides;
import com.linkedin.search.template.SearchFieldBoost;
import com.linkedin.search.template.SearchFieldBoostArray;
import com.linkedin.search.template.SearchFieldOverrides;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * PDL query-by-default fields, then a template add, remove, replace, or override. The indexed name
 * is {@code searchLabel}, or the field name.
 */
public final class SearchTemplateFields {
  public static final String ENTITY_NAME_FIELD = "name";
  public static final String DESCRIPTION_FIELD = "description";
  public static final String PLATFORM_FIELD = "platform";
  public static final String COLUMN_NAME_FIELD = "name_1";
  public static final String URN_FIELD = "urn";

  private static final Set<String> TEXT_FIELDS =
      Set.of("name", "qualifiedName", "description", "other");
  private static final Set<String> NAME_FIELDS = Set.of("name", "qualifiedName");

  private SearchTemplateFields() {}

  /** Degree-aware indexed name for the resolved V3 annotation, or its source field name. */
  @Nonnull
  public static String indexedName(@Nonnull SearchableAnnotation annotation) {
    SearchableAnnotation resolved = annotation.resolvedForV3();
    return resolved.indexedFieldName().orElse(resolved.getFieldName());
  }

  /** Serializes the already-resolved Stage 1 field configurations and their weights. */
  @Nonnull
  public static List<String> analyzedFields(@Nonnull List<SearchFieldConfig> fields) {
    List<String> names = new ArrayList<>();
    for (SearchFieldConfig field : fields) {
      names.add(field.fieldName() + "^" + trimBoost(field.boost()));
    }
    return names;
  }

  /**
   * Analyzed subfields of one indexed label. The label is already one degree: {@code qualifiedName}
   * does not include {@code qualifiedName_1}. A higher degree is its own list.
   */
  @Nonnull
  public static List<String> labelAnalyzedFields(
      @Nonnull List<SearchFieldConfig> fields, @Nonnull String label) {
    List<SearchFieldConfig> matched = new ArrayList<>();
    for (SearchFieldConfig field : fields) {
      if (label.equals(field.shortName()) && analyzedText(field.fieldName())) {
        matched.add(field);
      }
    }
    return analyzedFields(matched);
  }

  /**
   * Drops one indexed label and its degree copies from a shared best_fields list. {@code
   * qualifiedName_1} is not added to the degree-0 {@code qualifiedNameFields} list; that degree
   * needs its own list. Leaving the copy here lets a related field outrank the bounded degree-0
   * clause.
   */
  @Nonnull
  public static List<String> withoutLabel(@Nonnull List<String> analyzed, @Nonnull String label) {
    List<String> kept = new ArrayList<>();
    for (String entry : analyzed) {
      if (!label.equals(textBase(analyzedRoot(entry)))) {
        kept.add(entry);
      }
    }
    return kept;
  }

  /** Identity fields for a single word or a dotted name. */
  @Nonnull
  public static List<String> identityAnalyzedFields(@Nonnull List<SearchFieldConfig> fields) {
    List<SearchFieldConfig> identity = new ArrayList<>();
    for (SearchFieldConfig field : fields) {
      if (NAME_FIELDS.contains(textBase(field.fieldName()))
          || NAME_FIELDS.contains(field.shortName())) {
        identity.add(field);
      }
    }
    return analyzedFields(identity);
  }

  /** The exact field for an indexed name. A V3 keyword root is the exact field. */
  @Nonnull
  public static String keywordPath(@Nonnull String indexedName) {
    return indexedName;
  }

  @Nonnull
  public static String textPath(@Nonnull String indexedName) {
    if (TEXT_FIELDS.contains(textBase(indexedName))) {
      return indexedName + StructuredPropertySubfields.TEXT_SUFFIX;
    }
    return indexedName;
  }

  /** Indexed names this query searched. Highlighting scans those fields. */
  @Nonnull
  public static Set<String> highlightRoots(@Nonnull List<SearchFieldConfig> queried) {
    Set<String> names = new LinkedHashSet<>();
    for (SearchFieldConfig field : queried) {
      names.add(field.fieldName());
    }
    return names;
  }

  /** Analyzed text is a shared text field. A keyword is not. */
  public static boolean analyzedText(@Nonnull String indexedName) {
    return indexedName.endsWith(StructuredPropertySubfields.TEXT_SUFFIX)
        || indexedName.endsWith(StructuredPropertySubfields.STEMMED_SUFFIX);
  }

  @Nonnull
  private static String analyzedRoot(@Nonnull String analyzedEntry) {
    int caret = analyzedEntry.lastIndexOf('^');
    String name = caret > 0 ? analyzedEntry.substring(0, caret) : analyzedEntry;
    int dot = name.indexOf('.');
    return dot < 0 ? name : name.substring(0, dot);
  }

  @Nonnull
  public static String textBase(@Nonnull String indexedName) {
    int underscore = indexedName.lastIndexOf('_');
    if (underscore > 0) {
      String suffix = indexedName.substring(underscore + 1);
      if (!suffix.isEmpty() && suffix.chars().allMatch(Character::isDigit)) {
        return indexedName.substring(0, underscore);
      }
    }
    return indexedName;
  }

  /**
   * One weight per indexed name. The first annotation wins. Disagreeing weights are a schema
   * validation concern, not a query-time average.
   */
  @Nonnull
  public static List<SearchFieldConfig> pdlQueryFields(
      @Nonnull Collection<EntitySpec> entitySpecs) {
    List<SearchableAnnotation> annotations = new ArrayList<>();
    for (EntitySpec spec : entitySpecs) {
      for (SearchableFieldSpec fieldSpec : spec.getSearchableFieldSpecs()) {
        annotations.add(fieldSpec.getSearchableAnnotation());
      }
    }
    return queryFields(annotations);
  }

  @Nonnull
  public static List<SearchFieldConfig> queryFields(
      @Nonnull Collection<SearchableAnnotation> annotations) {
    Map<String, Float> boosts = new LinkedHashMap<>();
    for (SearchableAnnotation annotation : annotations) {
      SearchableAnnotation resolved = annotation.resolvedForV3();
      if (!resolved.isV3Searchable() || !resolved.isQueryByDefault()) {
        continue;
      }
      boosts.putIfAbsent(indexedName(resolved), (float) resolved.getBoostScore());
    }
    List<SearchFieldConfig> fields = new ArrayList<>();
    boosts.forEach(
        (name, boost) ->
            fields.add(
                SearchFieldConfig.detectSubFieldType(
                    name, boost, SearchableAnnotation.FieldType.KEYWORD, true)));
    return fields;
  }

  @Nonnull
  public static Set<String> pdlHighlightFields(@Nonnull Collection<EntitySpec> entitySpecs) {
    Set<String> names = new LinkedHashSet<>();
    for (SearchFieldConfig field : pdlQueryFields(entitySpecs)) {
      names.add(field.fieldName());
    }
    return names;
  }

  @Nonnull
  public static List<SearchFieldConfig> applyTemplate(
      @Nonnull List<SearchFieldConfig> base, @Nullable SearchFieldOverrides overrides) {
    return applyTemplate(base, overrides, StructuredPropertyReferences.identity());
  }

  @Nonnull
  public static List<SearchFieldConfig> applyTemplate(
      @Nonnull List<SearchFieldConfig> base,
      @Nullable SearchFieldOverrides overrides,
      @Nonnull StructuredPropertyReferences.Resolver resolver) {
    if (overrides == null || SearchTemplateLoader.emptyFields(overrides)) {
      return base;
    }
    boolean replace = nonemptyBoosts(overrides.hasReplace() ? overrides.getReplace() : null);
    boolean add = nonemptyBoosts(overrides.hasAdd() ? overrides.getAdd() : null);
    boolean remove = nonemptyStrings(overrides.hasRemove() ? overrides.getRemove() : null);
    boolean override = nonemptyBoosts(overrides.hasOverride() ? overrides.getOverride() : null);
    if (replace && (add || remove || override)) {
      throw new SearchTemplateException(
          "Search template fields.replace cannot be combined with add, remove, or override");
    }
    if (replace) {
      List<SearchFieldConfig> replaced = new ArrayList<>();
      for (SearchFieldBoost boost : overrides.getReplace()) {
        SearchFieldConfig config = config(boost, resolver);
        if (!config.fieldName().isBlank()) {
          replaced.add(config);
        }
      }
      return replaced;
    }
    Map<String, SearchFieldConfig> fields = new LinkedHashMap<>();
    for (SearchFieldConfig field : base) {
      fields.put(field.fieldName(), field);
    }
    if (remove) {
      for (String name : overrides.getRemove()) {
        String resolved = resolver.resolveRemove(name);
        if (!resolved.isBlank()) {
          fields.entrySet().removeIf(entry -> sameLogicalField(entry.getKey(), resolved));
        }
      }
    }
    if (add) {
      for (SearchFieldBoost boost : overrides.getAdd()) {
        SearchFieldConfig config = config(boost, resolver);
        if (!config.fieldName().isBlank()) {
          fields.put(config.fieldName(), config);
        }
      }
    }
    if (override) {
      for (SearchFieldBoost boost : overrides.getOverride()) {
        SearchFieldConfig config = config(boost, resolver);
        if (config.fieldName().isBlank()) {
          continue;
        }
        List<String> matching =
            fields.keySet().stream()
                .filter(name -> sameLogicalField(name, config.fieldName()))
                .toList();
        if (matching.isEmpty()) {
          throw new SearchTemplateException(
              "Search template fields.override is not in the queried set: " + boost.getFieldName());
        }
        matching.forEach(
            name -> fields.put(name, fields.get(name).toBuilder().boost(config.boost()).build()));
      }
    }
    return new ArrayList<>(fields.values());
  }

  private static boolean sameLogicalField(@Nonnull String configured, @Nonnull String requested) {
    if (configured.equals(requested)) {
      return true;
    }
    return configured.startsWith(requested + ".")
        && (configured.endsWith(StructuredPropertySubfields.TEXT_SUFFIX)
            || configured.endsWith(StructuredPropertySubfields.STEMMED_SUFFIX));
  }

  /**
   * Combines a parent's field section with a child's. A child {@code replace} discards the parent.
   * Otherwise {@code add}, {@code remove}, and {@code override} merge by resolved field name, and
   * the child's override wins. When the parent used {@code replace}, the child's keys apply to that
   * set and the result stays a {@code replace}.
   */
  @Nullable
  public static SearchFieldOverrides merge(
      @Nullable SearchFieldOverrides parent,
      @Nullable SearchFieldOverrides child,
      @Nonnull StructuredPropertyReferences.Resolver resolver) {
    if (child == null || SearchTemplateLoader.emptyFields(child)) {
      return child == null ? parent : null;
    }
    boolean replace = nonemptyBoosts(child.hasReplace() ? child.getReplace() : null);
    boolean add = nonemptyBoosts(child.hasAdd() ? child.getAdd() : null);
    boolean remove = nonemptyStrings(child.hasRemove() ? child.getRemove() : null);
    boolean override = nonemptyBoosts(child.hasOverride() ? child.getOverride() : null);
    if (replace && (add || remove || override)) {
      throw new SearchTemplateException(
          "Search template fields.replace cannot be combined with add, remove, or override");
    }
    if (replace) {
      SearchFieldOverrides replaced = new SearchFieldOverrides();
      replaced.setReplace(copyBoosts(child.getReplace()));
      return replaced;
    }
    boolean parentReplace =
        parent != null && nonemptyBoosts(parent.hasReplace() ? parent.getReplace() : null);
    if (parentReplace) {
      Map<String, SearchFieldBoost> set = new LinkedHashMap<>();
      for (SearchFieldBoost boost : parent.getReplace()) {
        putBoost(set, boost, resolver);
      }
      if (remove) {
        for (String name : child.getRemove()) {
          set.remove(resolver.resolveRemove(name));
        }
      }
      if (add) {
        for (SearchFieldBoost boost : child.getAdd()) {
          putBoost(set, boost, resolver);
        }
      }
      if (override) {
        for (SearchFieldBoost boost : child.getOverride()) {
          String name =
              resolver.resolve(
                  boost.getFieldName(), boost.hasSubfield() ? boost.getSubfield() : null);
          if (!set.containsKey(name)) {
            throw new SearchTemplateException(
                "Search template fields.override is not in the queried set: "
                    + boost.getFieldName());
          }
          putBoost(set, boost, resolver);
        }
      }
      SearchFieldOverrides merged = new SearchFieldOverrides();
      merged.setReplace(new SearchFieldBoostArray(set.values()));
      return merged;
    }
    Map<String, SearchFieldBoost> adds = new LinkedHashMap<>();
    List<String> removes = new ArrayList<>();
    Map<String, SearchFieldBoost> overrides = new LinkedHashMap<>();
    if (parent != null) {
      if (nonemptyBoosts(parent.hasAdd() ? parent.getAdd() : null)) {
        for (SearchFieldBoost boost : parent.getAdd()) {
          putBoost(adds, boost, resolver);
        }
      }
      if (nonemptyStrings(parent.hasRemove() ? parent.getRemove() : null)) {
        for (String name : parent.getRemove()) {
          removes.add(resolver.resolveRemove(name));
        }
      }
      if (nonemptyBoosts(parent.hasOverride() ? parent.getOverride() : null)) {
        for (SearchFieldBoost boost : parent.getOverride()) {
          putBoost(overrides, boost, resolver);
        }
      }
    }
    if (remove) {
      for (String name : child.getRemove()) {
        String resolved = resolver.resolveRemove(name);
        if (!removes.contains(resolved)) {
          removes.add(resolved);
        }
        adds.remove(resolved);
        overrides.remove(resolved);
      }
    }
    if (add) {
      for (SearchFieldBoost boost : child.getAdd()) {
        String name = putBoost(adds, boost, resolver);
        removes.remove(name);
      }
    }
    if (override) {
      for (SearchFieldBoost boost : child.getOverride()) {
        String name = putBoost(overrides, boost, resolver);
        if (removes.contains(name) && !adds.containsKey(name)) {
          throw new SearchTemplateException(
              "Search template fields.override is not in the queried set: " + boost.getFieldName());
        }
      }
    }
    SearchFieldOverrides merged = new SearchFieldOverrides();
    if (!adds.isEmpty()) {
      merged.setAdd(new SearchFieldBoostArray(adds.values()));
    }
    if (!removes.isEmpty()) {
      merged.setRemove(new StringArray(removes));
    }
    if (!overrides.isEmpty()) {
      merged.setOverride(new SearchFieldBoostArray(overrides.values()));
    }
    return SearchTemplateLoader.emptyFields(merged) ? null : merged;
  }

  @Nonnull
  public static List<String> applyHighlight(
      @Nonnull Set<String> base, @Nullable HighlightFieldOverrides overrides) {
    if (overrides == null || SearchTemplateLoader.emptyHighlights(overrides)) {
      return new ArrayList<>(base);
    }
    boolean replace = overrides.hasReplace() && !overrides.getReplace().isEmpty();
    boolean add = overrides.hasAdd() && !overrides.getAdd().isEmpty();
    boolean remove = overrides.hasRemove() && !overrides.getRemove().isEmpty();
    if (replace && (add || remove)) {
      throw new SearchTemplateException(
          "Search template highlightFields.replace cannot be combined with add or remove");
    }
    if (replace) {
      return new ArrayList<>(overrides.getReplace());
    }
    Set<String> fields = new LinkedHashSet<>(base);
    if (remove) {
      overrides.getRemove().forEach(fields::remove);
    }
    if (add) {
      fields.addAll(overrides.getAdd());
    }
    return new ArrayList<>(fields);
  }

  /**
   * A child add or remove changes the parent's resolved highlight set. An explicit empty section
   * drops the inherited set. Replace discards the parent set.
   */
  @Nullable
  public static HighlightFieldOverrides mergeHighlights(
      @Nullable HighlightFieldOverrides parent,
      @Nullable HighlightFieldOverrides child,
      @Nonnull StructuredPropertyReferences.Resolver resolver) {
    if (child == null || SearchTemplateLoader.emptyHighlights(child)) {
      return child == null ? parent : null;
    }
    boolean replace = child.hasReplace() && !child.getReplace().isEmpty();
    boolean add = child.hasAdd() && !child.getAdd().isEmpty();
    boolean remove = child.hasRemove() && !child.getRemove().isEmpty();
    if (replace && (add || remove)) {
      throw new SearchTemplateException(
          "Search template highlightFields.replace cannot be combined with add or remove");
    }
    if (replace) {
      HighlightFieldOverrides replaced = new HighlightFieldOverrides();
      replaced.setReplace(resolveHighlightNames(child.getReplace(), resolver));
      return replaced;
    }
    boolean parentReplace = parent != null && parent.hasReplace() && !parent.getReplace().isEmpty();
    LinkedHashSet<String> adds = new LinkedHashSet<>();
    LinkedHashSet<String> removes = new LinkedHashSet<>();
    if (parentReplace) {
      adds.addAll(resolveHighlightNames(parent.getReplace(), resolver));
    } else if (parent != null) {
      if (parent.hasAdd() && parent.getAdd() != null) {
        adds.addAll(resolveHighlightNames(parent.getAdd(), resolver));
      }
      if (parent.hasRemove() && parent.getRemove() != null) {
        removes.addAll(resolveHighlightNames(parent.getRemove(), resolver));
      }
    }
    if (remove) {
      for (String name : child.getRemove()) {
        String resolved = resolver.resolveRemove(name);
        adds.remove(resolved);
        removes.add(resolved);
      }
    }
    if (add) {
      for (String name : child.getAdd()) {
        String resolved = resolver.resolveRemove(name);
        removes.remove(resolved);
        adds.add(resolved);
      }
    }
    HighlightFieldOverrides merged = new HighlightFieldOverrides();
    if (parentReplace) {
      if (!adds.isEmpty()) {
        merged.setReplace(new StringArray(adds));
      }
    } else {
      if (!adds.isEmpty()) {
        merged.setAdd(new StringArray(adds));
      }
      if (!removes.isEmpty()) {
        merged.setRemove(new StringArray(removes));
      }
    }
    return SearchTemplateLoader.emptyHighlights(merged) ? null : merged;
  }

  @Nonnull
  private static StringArray resolveHighlightNames(
      @Nonnull StringArray names, @Nonnull StructuredPropertyReferences.Resolver resolver) {
    StringArray resolved = new StringArray();
    for (String name : names) {
      resolved.add(resolver.resolveRemove(name));
    }
    return resolved;
  }

  @Nonnull
  public static List<String> toBoostedNames(@Nonnull List<SearchFieldConfig> fields) {
    List<String> names = new ArrayList<>();
    for (SearchFieldConfig field : fields) {
      names.add(field.fieldName() + "^" + trimBoost(field.boost()));
    }
    return names;
  }

  @Nonnull
  private static SearchFieldConfig config(
      @Nonnull SearchFieldBoost boost, @Nonnull StructuredPropertyReferences.Resolver resolver) {
    return SearchFieldConfig.detectSubFieldType(
        resolver.resolve(boost.getFieldName(), subfield(boost)),
        boost.getBoost().floatValue(),
        SearchableAnnotation.FieldType.KEYWORD,
        true);
  }

  @Nullable
  private static String subfield(@Nonnull SearchFieldBoost boost) {
    return boost.hasSubfield() ? boost.getSubfield() : null;
  }

  @Nonnull
  public static String formatBoost(float boost) {
    return trimBoost(boost);
  }

  @Nonnull
  private static String putBoost(
      @Nonnull Map<String, SearchFieldBoost> fields,
      @Nonnull SearchFieldBoost boost,
      @Nonnull StructuredPropertyReferences.Resolver resolver) {
    String name = resolver.resolve(boost.getFieldName(), subfield(boost));
    fields.put(name, copyBoost(boost));
    return name;
  }

  @Nonnull
  private static SearchFieldBoostArray copyBoosts(@Nonnull SearchFieldBoostArray boosts) {
    SearchFieldBoostArray copy = new SearchFieldBoostArray();
    for (SearchFieldBoost boost : boosts) {
      copy.add(copyBoost(boost));
    }
    return copy;
  }

  @Nonnull
  private static SearchFieldBoost copyBoost(@Nonnull SearchFieldBoost boost) {
    SearchFieldBoost copy = new SearchFieldBoost();
    copy.setFieldName(boost.getFieldName());
    copy.setBoost(boost.getBoost());
    if (boost.hasSubfield() && boost.getSubfield() != null) {
      copy.setSubfield(boost.getSubfield());
    }
    return copy;
  }

  private static boolean nonemptyBoosts(@Nullable SearchFieldBoostArray boosts) {
    return boosts != null && !boosts.isEmpty();
  }

  private static boolean nonemptyStrings(@Nullable StringArray names) {
    return names != null && !names.isEmpty();
  }

  @Nonnull
  private static String trimBoost(float boost) {
    if (boost == (long) boost) {
      return Long.toString((long) boost);
    }
    return Float.toString(boost);
  }
}
