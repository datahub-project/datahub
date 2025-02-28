package com.linkedin.metadata.test.definition.value;

public class DateType implements ValueType {
  private static final DateType INSTANCE = new DateType();

  @Override
  public boolean equals(Object o) {
    return this.getClass().equals(o.getClass());
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  public static DateType get() {
    return INSTANCE;
  }
}
