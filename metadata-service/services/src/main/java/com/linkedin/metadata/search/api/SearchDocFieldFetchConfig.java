package com.linkedin.metadata.search.api;

import com.linkedin.metadata.query.SearchFlags;
import java.util.LinkedHashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
      Set.of("urn", "usageCountLast30Days");

  private Set<String> fieldsToFetch = DEFAULT_FIELDS_TO_FETCH_ON_SCROLL;

  /**
   * Union {@code defaults} with {@code searchFlags.fetchExtraFields}, preserving insertion order
   * and dropping blank names.
   */
  @Nonnull
  public static Set<String> resolve(
      @Nonnull Set<String> defaults, @Nullable SearchFlags searchFlags) {
    LinkedHashSet<String> fields = new LinkedHashSet<>(defaults);
    if (searchFlags != null && searchFlags.getFetchExtraFields() != null) {
      for (String field : searchFlags.getFetchExtraFields()) {
        if (field != null && !field.isBlank()) {
          fields.add(field);
        }
      }
    }
    return fields;
  }
}
