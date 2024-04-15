package com.linkedin.metadata.aspect.patch.template.datajob;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.metadata.aspect.patch.template.Template;
import javax.annotation.Nonnull;

public class DataJobInfoTemplate implements Template<DataJobInfo> {

  @Override
  public DataJobInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof DataJobInfo) {
      return (DataJobInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to DataJobInfo");
  }

  @Override
  public Class<DataJobInfo> getTemplateType() {
    return DataJobInfo.class;
  }

  @Nonnull
  @Override
  public DataJobInfo getDefault() {
    DataJobInfo dataJobInfo = new DataJobInfo();
    dataJobInfo.setCustomProperties(new StringMap());

    return dataJobInfo;
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
