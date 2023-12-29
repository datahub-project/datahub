package com.linkedin.metadata.models.registry.template.dataflow;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.metadata.models.registry.template.Template;
import javax.annotation.Nonnull;

public class DataFlowInfoTemplate implements Template<DataFlowInfo> {

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
