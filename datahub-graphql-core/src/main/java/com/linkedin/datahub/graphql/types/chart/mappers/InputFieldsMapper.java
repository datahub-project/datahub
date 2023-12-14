package com.linkedin.datahub.graphql.types.chart.mappers;

import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.InputField;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaFieldMapper;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class InputFieldsMapper {

  public static final InputFieldsMapper INSTANCE = new InputFieldsMapper();

  public static com.linkedin.datahub.graphql.generated.InputFields map(
      @Nonnull final InputFields metadata, @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(metadata, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.InputFields apply(
      @Nonnull final InputFields input, @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.InputFields result =
        new com.linkedin.datahub.graphql.generated.InputFields();
    result.setFields(
        input.getFields().stream()
            .map(
                field -> {
                  InputField fieldResult = new InputField();

                  if (field.hasSchemaField()) {
                    fieldResult.setSchemaField(
                        SchemaFieldMapper.map(field.getSchemaField(), entityUrn));
                  }
                  if (field.hasSchemaFieldUrn()) {
                    fieldResult.setSchemaFieldUrn(field.getSchemaFieldUrn().toString());
                  }
                  return fieldResult;
                })
            .collect(Collectors.toList()));

    return result;
  }
}
