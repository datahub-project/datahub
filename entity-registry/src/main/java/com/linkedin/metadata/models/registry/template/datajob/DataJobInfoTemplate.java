package com.linkedin.metadata.models.registry.template.datajob;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.metadata.models.registry.template.Template;
import javax.annotation.Nonnull;

public class DataJobInfoTemplate implements Template<DataJobInfo> {

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
