package com.linkedin.metadata.test.query.schemafield;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * A resolved schema field query response, that is returned from the query evaluator for
 * `schemaFields`.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class SchemaField {
  /** The path of the field */
  private String path;

  /** The description of the field */
  private String description;

  /** The editable description of the field */
  private String editableDescription;
}
