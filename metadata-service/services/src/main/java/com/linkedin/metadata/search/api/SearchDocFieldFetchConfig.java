package com.linkedin.metadata.search.api;

import java.util.Set;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(fluent = true)
@EqualsAndHashCode
public class SearchDocFieldFetchConfig {

  public static final Set<String> DEFAULT_FIELDS_TO_FETCH_ON_SCROLL = Set.of("urn");
  public static final Set<String> DEFAULT_FIELDS_TO_FETCH_ON_SEARCH =
      Set.of(
          "urn",
          // Fields required for Stage 2 rescoring (rescore_config.yaml)
          "viewCountLast30DaysFeature",
          "queryCountLast30DaysFeature",
          "usageCountLast30DaysFeature",
          "uniqueUserCountLast30DaysFeature",
          "hasDescription",
          "description", // For hasDescription check (datasets, charts, etc.)
          "definition", // For hasDescription check (glossary terms use this field)
          "hasOwners",
          "hasTags",
          "hasGlossaryTerms",
          "lastModified",
          "deprecated");

  private Set<String> fieldsToFetch = DEFAULT_FIELDS_TO_FETCH_ON_SEARCH;
}
