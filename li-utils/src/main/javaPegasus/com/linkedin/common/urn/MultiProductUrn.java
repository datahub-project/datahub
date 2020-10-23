package com.linkedin.common.urn;

public final class MultiProductUrn extends Urn {

  public static final String ENTITY_TYPE = "multiProduct";

  private final String productNameEntity;

  public MultiProductUrn(String productName) {
    super(ENTITY_TYPE, productName);
    this.productNameEntity = productName;
  }

  public String getProductNameEntity() {
    return productNameEntity;
  }
}
