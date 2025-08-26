package com.linkedin.metadata.test.definition.literal;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.linkedin.metadata.test.definition.value.ListType;
import com.linkedin.metadata.test.definition.value.StringType;
import com.linkedin.metadata.test.definition.value.ValueType;
import java.util.List;
import lombok.Builder;
import lombok.ToString;
import lombok.Value;

/** Parameter with a list of simple string values (no resolution required) */
@Value
@ToString
@JsonDeserialize(builder = StringListLiteral.StringListLiteralBuilder.class)
@Builder
public class StringListLiteral implements Literal {

  @JsonProperty("values")
  /** The actual literal values */
  List<String> values;

  public StringListLiteral(List<String> values) {
    this.values = values;
  }

  @Override
  public Object value() {
    return this.values;
  }

  @Override
  public ValueType valueType() {
    return new ListType(new StringType());
  }

  @JsonPOJOBuilder(withPrefix = "")
  public static class StringListLiteralBuilder {}
}
