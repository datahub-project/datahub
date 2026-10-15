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
  public static final String COLUMN_NAME_FIELD = "columnName";
  public static final String URN_FIELD = "urn";
  public static final String CUSTOM_PROPERTIES_FIELD = "customProperties";
  public static final float CUSTOM_PROPERTIES_WEIGHT = 0.8f;

  private static final Set<String> TEXT_FIELDS =
      Set.of("name", "qualifiedName", "description", "columns");
  private static final Set<String> NAME_FIELDS = Set.of("name", "qualifiedName");

  @Nonnull
  public static SearchFieldConfig customPropertiesField() {
    return shortField(CUSTOM_PROPERTIES_FIELD, CUSTOM_PROPERTIES_WEIGHT);
  }

  private SearchTemplateFields() {}

  /**
   * Shared fields used to describe how a name is queried. The request's default set comes from
   * {@link #pdlQueryFields}, not from this list.
   */
  @Nonnull
  public static List<SearchFieldConfig> sharedDefaults() {
    List<SearchFieldConfig> fields = new ArrayList<>();
    fields.add(shortField("name", 10f));
    fields.add(shortField("qualifiedName", 10f));
    fields.add(shortField("description", 1f));
    fields.add(shortField("columns", 1f));
    fields.add(shortField("tags", 0.5f));
    return fields;
  }

  /** Indexed name for an annotation: {@code searchLabel}, or {@code fieldName}. */
  @Nonnull
  public static String indexedName(@Nonnull SearchableAnnotation annotation) {
    return annotation.indexedName();
  }

  /**
   * Text is queried at the field weight. Stemmed is half, except on the name fields. Other indexed
   * names are queried as themselves.
   */
  @Nonnull
  public static List<String> analyzedFields(@Nonnull List<SearchFieldConfig> fields) {
    List<String> names = new ArrayList<>();
    for (SearchFieldConfig field : fields) {
      String indexed = field.fieldName();
      if (indexed.indexOf('.') > 0) {
        names.add(indexed + "^" + trimBoost(field.boost()));
        continue;
      }
      String base = textBase(indexed);
      if (TEXT_FIELDS.contains(base)) {
        names.add(indexed + ".text^" + trimBoost(field.boost()));
        if (!NAME_FIELDS.contains(base)) {
          names.add(indexed + ".stemmed^" + trimBoost(field.boost() / 2f));
        }
      } else {
        names.add(indexed + "^" + trimBoost(field.boost()));
      }
    }
    return names;
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
    List<String> names = analyzedFields(identity);
    names.add(URN_FIELD + "^1");
    return names;
  }

  @Nonnull
  public static String keywordPath(@Nonnull String indexedName) {
    if (URN_FIELD.equals(indexedName)) {
      return URN_FIELD;
    }
    if (TEXT_FIELDS.contains(textBase(indexedName))) {
      return indexedName + ".keyword";
    }
    return indexedName;
  }

  @Nonnull
  public static String textPath(@Nonnull String indexedName) {
    if (TEXT_FIELDS.contains(textBase(indexedName))) {
      return indexedName + ".text";
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
    String base = indexedName;
    int dot = base.indexOf('.');
    if (dot > 0) {
      base = base.substring(0, dot);
    }
    return TEXT_FIELDS.contains(textBase(base));
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

  @Nonnull
  private static SearchFieldConfig shortField(@Nonnull String name, float boost) {
    return SearchFieldConfig.detectSubFieldType(
        name, boost, SearchableAnnotation.FieldType.KEYWORD, true);
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
      if (!annotation.isQueryByDefault()) {
        continue;
      }
      boosts.putIfAbsent(indexedName(annotation), (float) annotation.getBoostScore());
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
          fields.remove(resolved);
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
        if (!fields.containsKey(config.fieldName())) {
          throw new SearchTemplateException(
              "Search template fields.override is not in the queried set: " + boost.getFieldName());
        }
        fields.put(config.fieldName(), config);
      }
    }
    return new ArrayList<>(fields.values());
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
