package com.linkedin.metadata.generator;

import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.Data;


/***
 *  Getter & setter class for schema event metadata.
 */
@Data
public class EventSpec {
  // doc of the model, such as: For unit tests.
  protected String doc;

  // specType of the model, such as: MetadataChangeEvent.
  protected String specType;

  // fullValueType of the model, such as: com.linkedin.testing.AnnotatedAspectBaz.
  protected String fullValueType;

  // namespace of the model, such as: com.linkedin.testing.
  protected String namespace;

  // entities leverage the model, such as: com.linkedin.common.FooBarUrn.
  protected Set<String> urnSet = new HashSet<>();

  // valueType of the model, such as: AnnotatedAspectBaz.
  protected String valueType;

  public EventSpec() {
  }

  public boolean hasDoc() {
    return doc != null && !doc.isEmpty();
  }

  public void setValueType(@Nonnull String schemaFullName) {
    fullValueType = schemaFullName;
    valueType = SchemaGeneratorUtil.stripNamespace(schemaFullName);
  }
}