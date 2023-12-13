package com.linkedin.datahub.graphql.types.dataset.mappers;

import com.linkedin.common.urn.Urn;
import com.linkedin.schema.EditableSchemaMetadata;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class EditableSchemaMetadataMapper {

  public static final EditableSchemaMetadataMapper INSTANCE = new EditableSchemaMetadataMapper();

  public static com.linkedin.datahub.graphql.generated.EditableSchemaMetadata map(
      @Nonnull final EditableSchemaMetadata metadata, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(metadata, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.EditableSchemaMetadata apply(
      @Nonnull final EditableSchemaMetadata input, @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.EditableSchemaMetadata result =
        new com.linkedin.datahub.graphql.generated.EditableSchemaMetadata();
    result.setEditableSchemaFieldInfo(
        input.getEditableSchemaFieldInfo().stream()
            .map(schemaField -> EditableSchemaFieldInfoMapper.map(schemaField, entityUrn))
            .collect(Collectors.toList()));
    return result;
  }
}
