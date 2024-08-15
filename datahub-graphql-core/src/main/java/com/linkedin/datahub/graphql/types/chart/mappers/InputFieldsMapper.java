package com.linkedin.datahub.graphql.types.chart.mappers;

import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.InputField;
import com.linkedin.datahub.graphql.types.dataset.mappers.SchemaFieldMapper;
import java.net.URISyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InputFieldsMapper {

  public static final InputFieldsMapper INSTANCE = new InputFieldsMapper();

  public static com.linkedin.datahub.graphql.generated.InputFields map(
      @Nullable final QueryContext context,
      @Nonnull final InputFields metadata,
      @Nonnull final Urn entityUrn) {
    return INSTANCE.apply(context, metadata, entityUrn);
  }

  public com.linkedin.datahub.graphql.generated.InputFields apply(
      @Nullable final QueryContext context,
      @Nonnull final InputFields input,
      @Nonnull final Urn entityUrn) {
    final com.linkedin.datahub.graphql.generated.InputFields result =
        new com.linkedin.datahub.graphql.generated.InputFields();
    result.setFields(
        input.getFields().stream()
            .map(
                field -> {
                  InputField fieldResult = new InputField();
                  Urn parentUrn = entityUrn;

                  if (field.hasSchemaFieldUrn()) {
                    fieldResult.setSchemaFieldUrn(field.getSchemaFieldUrn().toString());
                    try {
                      parentUrn =
                          Urn.createFromString(field.getSchemaFieldUrn().getEntityKey().get(0));
                    } catch (URISyntaxException e) {
                      log.error(
                          "Field urn resolution: failed to extract parentUrn successfully from {}. Falling back to {}",
                          field.getSchemaFieldUrn(),
                          entityUrn,
                          e);
                    }
                  }
                  if (field.hasSchemaField()) {
                    fieldResult.setSchemaField(
                        SchemaFieldMapper.map(context, field.getSchemaField(), parentUrn));
                  }
                  return fieldResult;
                })
            .collect(Collectors.toList()));

    return result;
  }
}
