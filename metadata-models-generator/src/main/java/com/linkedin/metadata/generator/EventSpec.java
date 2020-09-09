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
  // delta model for partial update, such as: com.linkedin.datasetGroup.MembershipDelta.
  protected String delta;

  // fullValueType of the model, such as: com.linkedin.identity.CorpUserInfo.
  protected String fullValueType;

  // namespace of the model, such as: com.linkedin.identity.
  protected String namespace;

  // specType of the model, such as: MetadataChangeEvent.
  protected String specType;

  // entities leverage the model, such as: com.linkedin.common.CorpuserUrn.
  protected Set<String> urnSet = new HashSet<>();

  // valueType of the model, such as: CorpUserInfo.
  protected String valueType;

  public EventSpec() {
  }

  public boolean hasDelta() {
    return delta != null && !delta.isEmpty();
  }

  public void setValueType(@Nonnull String schemaFullName) {
    fullValueType = schemaFullName;
    valueType = SchemaGeneratorUtil.stripNamespace(schemaFullName);
  }
}