/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.aspect.patch.template.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class DataFlowInfoTemplate implements Template<DataFlowInfo> {

  @Override
  public DataFlowInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DataFlowInfo) {
      return (DataFlowInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataFlowInfo");
  }

  @Override
  public Class<DataFlowInfo> getTemplateType() {
    return DataFlowInfo.class;
  }

  @Nonnull
  @Override
  public DataFlowInfo getDefault() {
    DataFlowInfo dataFlowInfo = new DataFlowInfo();
    dataFlowInfo.setCustomProperties(new StringMap());

    return dataFlowInfo;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    return baseNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    return patched;
  }
}
