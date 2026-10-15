package com.linkedin.metadata.search.elasticsearch.query.template;

import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTY_MAPPING_FIELD;
import static com.linkedin.metadata.models.StructuredPropertyUtils.toElasticsearchFieldName;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.DataHubSearchConfig;
import com.linkedin.metadata.models.annotation.SearchableAnnotation;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchFieldConfig;
import com.linkedin.structured.StructuredPropertyDefinition;
import com.linkedin.util.Pair;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Structured properties that opted in to full text. Their values are copied into one shared field,
 * {@code customProperties}. A property that has not opted in is absent from that copy and from the
 * default field list.
 */
public final class StructuredPropertyFullTextFields {
  private static final Set<String> FIELDS = ConcurrentHashMap.newKeySet();

  private StructuredPropertyFullTextFields() {}

  public static void replace(
      @Nullable Collection<Pair<Urn, StructuredPropertyDefinition>> properties) {
    FIELDS.clear();
    if (properties == null) {
      return;
    }
    for (Pair<Urn, StructuredPropertyDefinition> pair : properties) {
      if (optedIn(pair.getValue())) {
        FIELDS.add(
            STRUCTURED_PROPERTY_MAPPING_FIELD
                + "."
                + toElasticsearchFieldName(pair.getKey(), pair.getValue()));
      }
    }
  }

  public static boolean any() {
    return !FIELDS.isEmpty();
  }

  /** Root fields scanned from {@code _source} for highlighting. */
  @Nonnull
  public static Set<String> sourceFields() {
    return Set.copyOf(FIELDS);
  }

  @Nonnull
  public static List<SearchFieldConfig> fields() {
    if (FIELDS.isEmpty()) {
      return List.of();
    }
    return List.of(
        SearchFieldConfig.detectSubFieldType(
            SearchTemplateFields.CUSTOM_PROPERTIES_FIELD,
            SearchTemplateFields.CUSTOM_PROPERTIES_WEIGHT,
            SearchableAnnotation.FieldType.KEYWORD,
            true));
  }

  static boolean optedIn(@Nullable StructuredPropertyDefinition definition) {
    if (definition == null || !definition.hasSearchConfiguration()) {
      return false;
    }
    DataHubSearchConfig config = definition.getSearchConfiguration();
    return config != null && Boolean.TRUE.equals(config.isFullTextSearch());
  }
}
