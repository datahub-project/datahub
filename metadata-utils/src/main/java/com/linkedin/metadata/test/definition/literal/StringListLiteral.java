package com.linkedin.metadata.test.definition.literal;

import com.linkedin.metadata.test.definition.value.ListType;
import com.linkedin.metadata.test.definition.value.StringType;
import com.linkedin.metadata.test.definition.value.ValueType;
import java.util.List;
import lombok.ToString;
import lombok.Value;

/** Parameter with a list of simple string values (no resolution required) */
@Value
@ToString
public class StringListLiteral implements Literal {

  /** The actual literal values */
  List<String> values;

  @Override
  public Object value() {
    return this.values;
  }

  @Override
  public ValueType valueType() {
    return new ListType(new StringType());
  }
}
