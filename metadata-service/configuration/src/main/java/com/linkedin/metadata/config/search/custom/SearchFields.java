package com.linkedin.metadata.config.search.custom;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Builder(toBuilder = true)
@Getter
@ToString
@EqualsAndHashCode
@JsonDeserialize(builder = SearchFields.SearchFieldsBuilder.class)
public class SearchFields {

  // Fields to add to the system defaults
  @Builder.Default private List<String> add = Collections.emptyList();

  // Fields to remove from the system defaults
  @Builder.Default private List<String> remove = Collections.emptyList();

  // Fields to completely replace the system defaults with
  @Builder.Default private List<String> replace = Collections.emptyList();

  @JsonPOJOBuilder(withPrefix = "")
  public static class SearchFieldsBuilder {}

  /**
   * Validates that operations are mutually exclusive with replace (add + remove is allowed, but
   * replace must be used alone)
   */
  @JsonIgnore
  public boolean isValid() {
    if (!replace.isEmpty()) {
      return add.isEmpty() && remove.isEmpty();
    }
    return true;
  }
}
