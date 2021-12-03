package com.linkedin.datahub.lineage.spark.model;

import java.util.Arrays;
import java.util.List;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.lineage.spark.interceptor.LineageUtils;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeProposal;

import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
public class SQLQueryExecEndEvent extends LineageEvent {

  private final long sqlQueryExecId;
  private final SQLQueryExecStartEvent start;

  public SQLQueryExecEndEvent(String master, String appName, String appId, long time, long sqlQueryExecId, SQLQueryExecStartEvent start) {
    super(master, appName, appId, time);
    this.sqlQueryExecId = sqlQueryExecId;
    this.start = start;
  }

  @Override
  public List<MetadataChangeProposal> toMcps() {
    DataJobUrn jobUrn = start.jobUrn();
    StringMap customProps = start.customProps();
    customProps.put("completedAt", timeStr());

    DataJobInfo jobInfo = start.jobInfo()
        .setCustomProperties(customProps);

    MetadataChangeProposal mcpJobInfo = new MetadataChangeProposal();
    mcpJobInfo.setAspectName("dataJobInfo");
    mcpJobInfo.setAspect(LineageUtils.serializeAspect(jobInfo));
    mcpJobInfo.setEntityUrn(jobUrn);
    mcpJobInfo.setEntityType("dataJob");
    mcpJobInfo.setChangeType(ChangeType.UPSERT);

    return Arrays.asList(mcpJobInfo);
  }
}