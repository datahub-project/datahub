package io.datahub.test.definition.value;

public class StringType implements ValueType {

  private static final StringType INSTANCE = new StringType();

  @Override
  public boolean equals(Object o) {
    return this.getClass().equals(o.getClass());
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  public static StringType get() {
    return INSTANCE;
  }

}
