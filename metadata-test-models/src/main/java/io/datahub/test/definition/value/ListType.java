package io.datahub.test.definition.value;

/**
 * Marker class
 */
public class ListType implements ValueType {

  private final ValueType itemType;

  public ListType(ValueType itemType) {
    this.itemType = itemType;
  }

  public ValueType getItemType() {
    return this.itemType;
  }

  @Override
  public boolean equals(Object o) {
    return this.getClass().equals(o.getClass()) && this.itemType.equals(((ListType) o).itemType);
  }

  @Override
  public int hashCode() {
    return this.getClass().hashCode() + this.itemType.hashCode();
  }
}
