/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.patch.template.dataset;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.dataset.DatasetProperties;
import com.linkedin.metadata.aspect.patch.template.ArrayMergingTemplate;
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
