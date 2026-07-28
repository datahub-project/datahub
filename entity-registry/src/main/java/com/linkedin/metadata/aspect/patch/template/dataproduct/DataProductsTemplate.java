package com.linkedin.metadata.aspect.patch.template.dataproduct;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.UrnArray;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataproduct.DataProducts;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Patch template for the asset-side {@code dataProducts} aspect (a plain {@code array[Urn]}). Keyed
 * by the URN value itself (empty key-field list), so ADD/REMOVE patch ops target {@code
 * /dataProducts/<dataProductUrn>}. Mirrors {@code DomainsTemplate}.
 */
public class DataProductsTemplate implements ArrayMergingTemplate<DataProducts> {

  private static final String DATA_PRODUCTS_FIELD_NAME = "dataProducts";

  @Override
  public DataProducts getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DataProducts) {
      return (DataProducts) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataProducts");
  }

  @Override
  public Class<DataProducts> getTemplateType() {
    return DataProducts.class;
  }

  @Nonnull
  @Override
  public DataProducts getDefault() {
    DataProducts dataProducts = new DataProducts();
    dataProducts.setDataProducts(new UrnArray());
    return dataProducts;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(baseNode, DATA_PRODUCTS_FIELD_NAME, Collections.emptyList());
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(patched, DATA_PRODUCTS_FIELD_NAME, Collections.emptyList());
  }
}
