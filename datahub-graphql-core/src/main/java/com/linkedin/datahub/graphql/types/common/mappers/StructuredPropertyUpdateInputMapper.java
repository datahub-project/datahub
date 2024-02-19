package com.linkedin.datahub.graphql.types.common.mappers;

import com.linkedin.datahub.graphql.generated.StructuredPropertyUpdateInput;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;

import javax.annotation.Nonnull;

public class StructuredPropertyUpdateInputMapper
    implements ModelMapper<StructuredPropertyUpdateInput, StructuredProperties> {

  public static final StructuredPropertyUpdateInputMapper INSTANCE =
      new StructuredPropertyUpdateInputMapper();

  public static StructuredProperties map(@Nonnull final StructuredPropertyUpdateInput updateInput) {
    return INSTANCE.apply(updateInput);
  }

  @Override
  public StructuredProperties apply(@Nonnull final StructuredPropertyUpdateInput updateInput) {
    StructuredProperties result = new StructuredProperties();
    StructuredPropertyValueAssignmentArray array = new StructuredPropertyValueAssignmentArray();
    result.setProperties(array);
    updateInput.getStructuredPropertyInputParams().forEach(param -> {
      array.add(StructuredPropertyInputParamsMapper.map(param));
    });
    return result;
  }
}
