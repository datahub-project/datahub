package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2LegacySettingsBuilder.NGRAM;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.DELIMITED;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_2;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_3;
import static com.linkedin.metadata.search.elasticsearch.index.entity.v2.V2MappingsBuilder.WORD_GRAMS_LENGTH_4;

import com.linkedin.data.template.StringArray;
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
   * Source paths for highlight names the query actually searches. Aspect paths and fields the query
   * does not read are left out. A structured property is its own path: {@code copy_to} does not put
   * its value on the shared field in {@code _source}, so a selected property is read where it is
   * stored.
   */
  @Nonnull
  public static List<String> sourcePaths(
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
    Map<String, List<String>> selected = new LinkedHashMap<>();
    List<String> properties = new ArrayList<>();
    for (String name : normalize(highlightNames)) {
      if (name.startsWith(V3SearchFields.STRUCTURED_PROPERTIES + ".")) {
        if (queried.contains(name) || structuredProperties.contains(name)) {
          properties.add(name);
        }
      } else if (queried.contains(name) && searched.containsKey(name)) {
        selected.put(name, searched.get(name));
      }
    }
    List<String> paths = new ArrayList<>();
    for (String path : V3SearchFields.matchedFieldSources(selected)) {
      if (!path.startsWith("_aspects")) {
        paths.add(path);
      }
    }
    paths.addAll(properties);
    return paths;
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
