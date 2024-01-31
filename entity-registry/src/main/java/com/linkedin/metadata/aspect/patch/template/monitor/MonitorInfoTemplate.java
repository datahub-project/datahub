package com.linkedin.metadata.aspect.patch.template.monitor;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.metadata.aspect.patch.template.Template;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import javax.annotation.Nonnull;

public class MonitorInfoTemplate implements Template<MonitorInfo> {

  @Override
  public MonitorInfo getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof MonitorInfo) {
      return (MonitorInfo) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to MonitorInfo");
  }

  @Override
  public Class<MonitorInfo> getTemplateType() {
    return MonitorInfo.class;
  }

  @Nonnull
  @Override
  public MonitorInfo getDefault() {
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);
    monitorInfo.setCustomProperties(new StringMap());
    monitorInfo.setStatus(new MonitorStatus().setMode(MonitorMode.INACTIVE));
    return monitorInfo;
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
