package com.linkedin.metadata.aspect.patch.template.dataproduct;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DataProductPropertiesTemplate implements ArrayMergingTemplate<DataProductProperties> {

  public static final String ASSETS_FIELD_NAME = "assets";
  public static final String KEY_FIELD_NAME = "destinationUrn";

  @Override
  public DataProductProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DataProductProperties) {
      return (DataProductProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataProductProperties");
  }

  @Override
  public Class<DataProductProperties> getTemplateType() {
    return DataProductProperties.class;
  }

  @Nonnull
  @Override
  public DataProductProperties getDefault() {
    DataProductProperties dataProductProperties = new DataProductProperties();
    dataProductProperties.setAssets(new DataProductAssociationArray());
    return dataProductProperties;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(baseNode, ASSETS_FIELD_NAME, Collections.singletonList(KEY_FIELD_NAME));
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(
        patched, ASSETS_FIELD_NAME, Collections.singletonList(KEY_FIELD_NAME));
  }
}
