package com.linkedin.datahub.lineage.spark.model;

import java.util.Arrays;
import java.util.List;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.lineage.spark.interceptor.LineageUtils;
import com.linkedin.datajob.DataFlowInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class AppStartEvent extends LineageEvent {

  private final String sparkUser;

  public AppStartEvent(String master, String appName, String appId, long time, String sparkUser) {
    super(master, appName, appId, time);
    this.sparkUser = sparkUser;
  }

  @Override
  public List<MetadataChangeProposal> toMcps() {
    DataFlowUrn flowUrn = LineageUtils.flowUrn(getMaster(), getAppName());

    DataFlowInfo flowInfo = new DataFlowInfo()
        .setName(getAppName())
        .setCustomProperties(customProps());

    MetadataChangeProposal mcpFlowInfo = new MetadataChangeProposal();
    mcpFlowInfo.setAspectName("dataFlowInfo");
    mcpFlowInfo.setAspect(LineageUtils.serializeAspect(flowInfo));
    mcpFlowInfo.setEntityUrn(flowUrn);
    mcpFlowInfo.setEntityType("dataFlow");
    mcpFlowInfo.setChangeType(ChangeType.UPSERT);
    return Arrays.asList(mcpFlowInfo);
  }

  StringMap customProps() {
    StringMap customProps = new StringMap();
    customProps.put("startedAt", timeStr());
    customProps.put("appId", getAppId());
    customProps.put("appName", getAppName());
    customProps.put("sparkUser", sparkUser);
    return customProps;
  }
}