package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NGRAM;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.DELIMITED;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_2;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_3;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_4;

import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.V3SearchFields;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFields;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.search.template.HighlightFieldOverrides;
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
 * The Search V3 highlight set. It starts empty unless a template or caller opts in. {@code
 * customHighlightingFields} replaces that set. {@code skipHighlighting} clears it.
 */
public final class SearchHighlightSelection {
  private static final Set<String> STRIPPED_SUFFIXES =
      Set.of(
          "*",
          DELIMITED,
          ESUtils.KEYWORD,
          NGRAM,
          "_2gram",
          "_3gram",
          "_4gram",
          WORD_GRAMS_LENGTH_2,
          WORD_GRAMS_LENGTH_3,
          WORD_GRAMS_LENGTH_4,
          "text",
          "stemmed");

  private SearchHighlightSelection() {}

  /**
   * Template names, then the request adjustment. A caller {@code customHighlightingFields} list
   * replaces the template set instead of intersecting with it.
   */
  @Nonnull
  public static List<String> resolve(
      @Nonnull Collection<String> templateNames, @Nullable SearchFlags flags) {
    List<String> template = normalize(templateNames);
    if (flags == null) {
      return template;
    }
    List<String> resolved = template;
    if (flags.hasCustomHighlightingFields()) {
      resolved = normalize(flags.getCustomHighlightingFields());
    } else if (flags.hasHighlightFields() && flags.getHighlightFields() != null) {
      resolved = applyRequest(template, flags.getHighlightFields());
    }
    if (Boolean.TRUE.equals(flags.isSkipHighlighting())) {
      return List.of();
    }
    return resolved;
  }

  /**
   * Fields to fetch for highlight names the query actually searches. A label whose value is stored
   * on that label is fetched under the label. Every other stored path of a queried label is fetched
   * too, including {@code other} and paths under {@code _aspects}, so a hit that does not store the
   * label can be read from the source that filled it. The {@code other} sink itself is not fetched.
   * A structured property is its own path.
   */
  @Nonnull
  public static List<String> sourcePaths(
      @Nonnull Collection<String> highlightNames,
      @Nonnull Map<String, List<String>> searched,
      @Nonnull Collection<String> queriedRoots,
      @Nonnull Collection<String> structuredProperties) {
    return fields(highlightNames, searched, queriedRoots, structuredProperties).fetchPaths();
  }

  /**
   * Highlight labels the rendered query reads, split into the label key and the stored paths that
   * fill it.
   */
  @Nonnull
  public static FieldSources fields(
      @Nonnull Collection<String> highlightNames,
      @Nonnull Map<String, List<String>> searched,
      @Nonnull Collection<String> queriedRoots,
      @Nonnull Collection<String> structuredProperties) {
    Set<String> queried = new LinkedHashSet<>();
    for (String root : queriedRoots) {
      String indexed = indexedRoot(root);
      if (indexed != null) {
        queried.add(indexed);
      }
    }
    String degreeOneName = SearchableAnnotation.indexedFieldName(V3SearchFields.ENTITY_NAME, 1);
    List<String> labelKeys = new ArrayList<>();
    List<String> fallbackPaths = new ArrayList<>();
    Map<String, String> fallbackLabels = new LinkedHashMap<>();
    List<String> direct = new ArrayList<>();
    for (String name : normalize(highlightNames)) {
      if (name.startsWith(V3SearchFields.STRUCTURED_PROPERTIES + ".")) {
        if (queried.contains(name) || structuredProperties.contains(name)) {
          direct.add(name);
        }
        continue;
      }
      if (degreeOneName.equals(name) || !queried.contains(name) || !searched.containsKey(name)) {
        continue;
      }
      List<String> sources = searched.get(name);
      boolean storedOnLabel = !V3SearchFields.OTHER.equals(name) && sources.contains(name);
      if (storedOnLabel) {
        labelKeys.add(name);
      }
      for (String path : sources) {
        if (V3SearchFields.OTHER.equals(path)
            || isColumnArray(path)
            || (storedOnLabel && name.equals(path))
            || fallbackLabels.containsKey(path)) {
          continue;
        }
        fallbackPaths.add(path);
        fallbackLabels.put(path, name);
      }
    }
    return new FieldSources(labelKeys, fallbackPaths, fallbackLabels, direct);
  }

  /** Label keys, the stored paths behind them, and structured-property paths. */
  public record FieldSources(
      @Nonnull List<String> labelKeys,
      @Nonnull List<String> fallbackPaths,
      @Nonnull Map<String, String> fallbackLabels,
      @Nonnull List<String> direct) {
    public static final FieldSources NONE =
        new FieldSources(List.of(), List.of(), Map.of(), List.of());

    public FieldSources {
      labelKeys = List.copyOf(labelKeys);
      fallbackPaths = List.copyOf(fallbackPaths);
      fallbackLabels = Map.copyOf(fallbackLabels);
      direct = List.copyOf(direct);
    }

    @Nonnull
    public List<String> fetchPaths() {
      LinkedHashSet<String> paths = new LinkedHashSet<>();
      paths.addAll(labelKeys);
      paths.addAll(fallbackPaths);
      paths.addAll(direct);
      return List.copyOf(paths);
    }
  }

  private static boolean isColumnArray(@Nonnull String path) {
    int separator = path.lastIndexOf('.');
    String fieldName = separator < 0 ? path : path.substring(separator + 1);
    return V3SearchFields.COLUMN_ARRAY_FIELDS.contains(fieldName);
  }

  @Nullable
  public static String indexedRoot(@Nullable String name) {
    if (name == null || name.isBlank() || name.indexOf("_aspects") >= 0) {
      return null;
    }
    String field = name;
    int boost = field.lastIndexOf('^');
    if (boost > 0) {
      field = field.substring(0, boost);
    }
    int separator;
    while ((separator = field.lastIndexOf('.')) > 0
        && STRIPPED_SUFFIXES.contains(field.substring(separator + 1))) {
      field = field.substring(0, separator);
    }
    if (field.isBlank() || field.indexOf('*') >= 0 || field.indexOf("_aspects") >= 0) {
      return null;
    }
    return field;
  }

  @Nonnull
  private static List<String> applyRequest(
      @Nonnull List<String> template, @Nonnull HighlightFieldOverrides request) {
    boolean replace =
        request.hasReplace() && request.getReplace() != null && !request.getReplace().isEmpty();
    boolean add = request.hasAdd() && request.getAdd() != null && !request.getAdd().isEmpty();
    boolean remove =
        request.hasRemove() && request.getRemove() != null && !request.getRemove().isEmpty();
    if (!replace && !add && !remove) {
      return List.of();
    }
    if (replace && (add || remove)) {
      throw new SearchTemplateException(
          "Search highlightFields.replace cannot be combined with add or remove");
    }
    if (replace) {
      return normalize(request.getReplace());
    }
    HighlightFieldOverrides normalized = new HighlightFieldOverrides();
    if (add) {
      normalized.setAdd(new StringArray(normalize(request.getAdd())));
    }
    if (remove) {
      normalized.setRemove(new StringArray(normalize(request.getRemove())));
    }
    return normalize(
        SearchTemplateFields.applyHighlight(new LinkedHashSet<>(template), normalized));
  }

  @Nonnull
  private static List<String> normalize(@Nullable Collection<String> names) {
    if (names == null || names.isEmpty()) {
      return List.of();
    }
    List<String> normalized = new ArrayList<>();
    for (String name : names) {
      String root = indexedRoot(name);
      if (root != null && !normalized.contains(root)) {
        normalized.add(root);
      }
    }
    return normalized;
  }
}
