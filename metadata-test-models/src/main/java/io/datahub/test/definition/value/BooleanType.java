package io.datahub.test.definition.value;

/**
 * Marker class
 */
public class BooleanType implements ValueType {

  private static final BooleanType INSTANCE = new BooleanType();

  @Override
  public boolean equals(Object o) {
    return this.getClass().equals(o.getClass());
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode();
  }

  public static BooleanType get() {
    return INSTANCE;
  }
}
