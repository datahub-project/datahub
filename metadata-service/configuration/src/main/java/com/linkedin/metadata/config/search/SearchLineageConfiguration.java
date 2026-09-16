package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class SearchLineageConfiguration {

  /**
   * How many parent datasets AUTO schema field validation is willing to read schemaMetadata for.
   * The fetch is batched and schema fields share parents heavily, so this is a backstop against a
   * walk that fans out over thousands of datasets rather than a limit normal queries approach.
   */
  private int maxParentsToValidate;
}
