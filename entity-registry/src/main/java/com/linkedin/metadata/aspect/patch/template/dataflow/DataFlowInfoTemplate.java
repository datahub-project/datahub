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
