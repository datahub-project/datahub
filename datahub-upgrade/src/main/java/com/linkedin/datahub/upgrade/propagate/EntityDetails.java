package com.linkedin.datahub.upgrade.propagate;

import com.linkedin.common.urn.Urn;
import com.linkedin.schema.EditableSchemaMetadata;
import com.linkedin.schema.SchemaMetadata;
import javax.annotation.Nullable;
import lombok.Value;


@Value
public class EntityDetails {
  Urn urn;
  @Nullable
  SchemaMetadata schemaMetadata;
  @Nullable
  EditableSchemaMetadata editableSchemaMetadata;
}