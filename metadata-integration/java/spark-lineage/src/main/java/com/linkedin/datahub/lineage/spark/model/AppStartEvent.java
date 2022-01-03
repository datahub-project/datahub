package com.linkedin.datahub.lineage.spark.model;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.lineage.spark.interceptor.LineageUtils;
import com.linkedin.datajob.DataFlowInfo;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Collections;
import java.util.List;
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
  public List<MetadataChangeProposalWrapper> toMcps() {
    DataFlowUrn flowUrn = LineageUtils.flowUrn(getMaster(), getAppName());

    DataFlowInfo flowInfo = new DataFlowInfo().setName(getAppName()).setCustomProperties(customProps());

    return Collections.singletonList(MetadataChangeProposalWrapper.create(
        b -> b.entityType("dataFlow").entityUrn(flowUrn).upsert().aspect(flowInfo)));
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