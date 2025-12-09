package com.linkedin.metadata.search.semantic;

import lombok.Builder;
import lombok.Data;

/**
 * Represents a text field in an entity aspect that is searchable and should be included in semantic
 * search embeddings. Extracted from PDL files based on @Searchable annotations with fieldType:
 * TEXT.
 */
@Data
@Builder
public class SearchableTextField {
  /**
   * The JSON path to the field in the document, e.g., "description" or
   * "schemaMetadata.fields.description"
   */
  private String fieldPath;

  /** The aspect name this field belongs to, e.g., "datasetProperties" */
  private String aspectName;

  /** Whether this field is within a nested/array structure */
  private boolean nested;

  /** Optional maximum length hint from the annotation */
  private Integer maxLength;

  /** The searchable field name used in Elasticsearch index */
  private String searchableFieldName;
}
