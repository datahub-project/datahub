package com.linkedin.metadata.aspect.patch.template.dataproduct;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProducts;
import com.linkedin.identity.CorpUserInfo;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonPatch;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DataProductsTemplateTest {

  private static final DataProductsTemplate TEMPLATE = new DataProductsTemplate();
  private static final String PRODUCT_A = "urn:li:dataProduct:a";
  private static final String PRODUCT_B = "urn:li:dataProduct:b";

  private static DataProducts initial(String... productUrns) {
    DataProductAssociationArray array = new DataProductAssociationArray();
    for (String urn : productUrns) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(UrnUtils.getUrn(urn));
      array.add(association);
    }
    return new DataProducts().setDataProducts(array);
  }

  private static JsonPatch addOp(String productUrn, boolean outputPort) {
    JsonObject value =
        Json.createObjectBuilder()
            .add("destinationUrn", productUrn)
            .add("outputPort", outputPort)
            .build();
    return Json.createPatch(
        Json.createArrayBuilder()
            .add(
                Json.createObjectBuilder()
                    .add("op", "add")
                    .add("path", "/dataProducts/" + productUrn)
                    .add("value", value))
            .build());
  }

  private static JsonPatch removeOp(String productUrn) {
    return Json.createPatch(
        Json.createArrayBuilder()
            .add(
                Json.createObjectBuilder()
                    .add("op", "remove")
                    .add("path", "/dataProducts/" + productUrn))
            .build());
  }

  private static List<String> urns(DataProducts dataProducts) {
    return dataProducts.getDataProducts().stream()
        .map(assoc -> assoc.getDestinationUrn().toString())
        .collect(Collectors.toList());
  }

  @Test
  public void testGetDefaultIsEmpty() {
    DataProducts result = TEMPLATE.getDefault();
    Assert.assertNotNull(result.getDataProducts());
    Assert.assertTrue(result.getDataProducts().isEmpty());
  }

  @Test
  public void testGetSubtypeValid() {
    DataProducts dataProducts = initial(PRODUCT_A);
    Assert.assertSame(TEMPLATE.getSubtype(dataProducts), dataProducts);
    Assert.assertEquals(TEMPLATE.getTemplateType(), DataProducts.class);
  }

  @Test(expectedExceptions = ClassCastException.class)
  public void testGetSubtypeWrongType() {
    TEMPLATE.getSubtype(new CorpUserInfo());
  }

  @Test
  public void testAddTwoDistinctEntries() throws Exception {
    DataProducts afterA = TEMPLATE.applyPatch(initial(), addOp(PRODUCT_A, false));
    DataProducts result = TEMPLATE.applyPatch(afterA, addOp(PRODUCT_B, true));

    List<String> productUrns = urns(result);
    Assert.assertEquals(productUrns.size(), 2);
    Assert.assertTrue(productUrns.contains(PRODUCT_A));
    Assert.assertTrue(productUrns.contains(PRODUCT_B));
    Assert.assertTrue(
        result.getDataProducts().stream()
            .anyMatch(a -> PRODUCT_B.equals(a.getDestinationUrn().toString()) && a.isOutputPort()));
  }

  @Test
  public void testAddIsIdempotent() throws Exception {
    DataProducts afterA = TEMPLATE.applyPatch(initial(), addOp(PRODUCT_A, false));
    DataProducts result = TEMPLATE.applyPatch(afterA, addOp(PRODUCT_A, false));

    Assert.assertEquals(urns(result), List.of(PRODUCT_A));
  }

  @Test
  public void testRemoveOneOfTwoEntries() throws Exception {
    DataProducts result = TEMPLATE.applyPatch(initial(PRODUCT_A, PRODUCT_B), removeOp(PRODUCT_A));

    List<String> productUrns = urns(result);
    Assert.assertEquals(productUrns.size(), 1);
    Assert.assertFalse(productUrns.contains(PRODUCT_A));
    Assert.assertTrue(productUrns.contains(PRODUCT_B));
  }
}
