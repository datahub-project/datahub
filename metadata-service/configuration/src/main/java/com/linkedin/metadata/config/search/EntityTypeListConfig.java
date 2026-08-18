package com.linkedin.metadata.config.search;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Configurable comma-separated registry entity-type list ({@code value} / {@code add} / {@code
 * remove}). Used for search defaults, autocomplete, browse, and quick-filter priority lists.
 * Production defaults live in {@code application.yaml}.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class EntityTypeListConfig {

  /**
   * Mirrors {@code elasticsearch.search.defaultEntityTypes.value} in {@code application.yaml}. Kept
   * in sync for tests and docs — production still loads from YAML.
   */
  public static final String DEFAULT_SEARCH_ENTITY_TYPES =
      "dataset,dashboard,chart,mlModel,mlModelGroup,mlFeatureTable,mlFeature,mlPrimaryKey,dataFlow,dataJob,glossaryTerm,glossaryNode,tag,role,corpuser,corpGroup,container,domain,dataProduct,notebook,businessAttribute,schemaField,application,document";

  /**
   * Mirrors {@code elasticsearch.search.autocompleteEntityTypes.value} in {@code application.yaml}.
   */
  public static final String DEFAULT_AUTOCOMPLETE_ENTITY_TYPES =
      "dataset,dashboard,chart,container,mlModel,mlModelGroup,mlFeatureTable,dataFlow,dataJob,glossaryTerm,tag,corpuser,corpGroup,notebook,dataProduct,domain,businessAttribute,application,structuredProperty";

  /** Mirrors {@code elasticsearch.search.browseEntityTypes.value} in {@code application.yaml}. */
  public static final String DEFAULT_BROWSE_ENTITY_TYPES =
      "dataset,dashboard,chart,container,mlModel,mlModelGroup,mlFeatureTable,dataFlow,dataJob,notebook,document";

  /**
   * Mirrors {@code elasticsearch.search.prioritizedSourceEntityTypes.value} in {@code
   * application.yaml}.
   */
  public static final String DEFAULT_PRIORITIZED_SOURCE_ENTITY_TYPES =
      "dataset,dashboard,dataFlow,dataJob,chart,container,mlModel,mlModelGroup,mlFeature,mlFeatureTable,mlPrimaryKey";

  /**
   * Mirrors {@code elasticsearch.search.prioritizedDatahubEntityTypes.value} in {@code
   * application.yaml}.
   */
  public static final String DEFAULT_PRIORITIZED_DATAHUB_ENTITY_TYPES =
      "domain,glossaryTerm,corpGroup,corpuser";

  /**
   * Optional comma-separated registry names that become the configured list. When empty / unset
   * with empty add/remove, the effective list is empty — GraphQL treats that as searching no entity
   * types (not all indices).
   */
  private String value;

  /** Comma-separated registry names to append to the effective list. */
  private String add;

  /** Comma-separated registry names to remove from the effective list. */
  private String remove;

  @JsonIgnore
  public boolean isEmpty() {
    return parseCsv(value).isEmpty() && parseCsv(add).isEmpty() && parseCsv(remove).isEmpty();
  }

  /**
   * Parses a comma-separated entity-type CSV into an ordered, case-folded, de-duplicated list.
   * First occurrence wins when the same type appears more than once (ignoring case).
   */
  public static List<String> parseCsv(String csv) {
    if (csv == null || csv.isBlank()) {
      return Collections.emptyList();
    }
    return Arrays.stream(csv.split(","))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .map(s -> s.toLowerCase(Locale.ROOT))
        .collect(
            Collectors.collectingAndThen(
                Collectors.toCollection(LinkedHashSet::new), List::copyOf));
  }

  public List<String> parsedValue() {
    return parseCsv(value);
  }

  public List<String> parsedAdd() {
    return parseCsv(add);
  }

  public List<String> parsedRemove() {
    return parseCsv(remove);
  }
}
