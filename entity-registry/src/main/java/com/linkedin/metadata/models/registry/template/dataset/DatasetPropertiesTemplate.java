package com.linkedin.metadata.models.registry.template.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.models.registry.template.ArrayMergingTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

public class DatasetPropertiesTemplate implements ArrayMergingTemplate<DatasetProperties> {

  private static final String TAGS_FIELD_NAME = "tags";

  @Override
  public DatasetProperties getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DatasetProperties) {
      return (DatasetProperties) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DatasetProperties");
  }

  @Override
  public Class<DatasetProperties> getTemplateType() {
    return DatasetProperties.class;
  }

  @Nonnull
  @Override
  public DatasetProperties getDefault() {
    DatasetProperties datasetProperties = new DatasetProperties();
    datasetProperties.setTags(new StringArray());
    datasetProperties.setCustomProperties(new StringMap());

    return datasetProperties;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return arrayFieldToMap(baseNode, TAGS_FIELD_NAME, Collections.emptyList());
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return transformedMapToArray(patched, TAGS_FIELD_NAME, Collections.emptyList());
  }
}
