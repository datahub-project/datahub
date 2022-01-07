package com.linkedin.datahub.lineage.spark.model;

import java.util.Date;
import java.util.List;

import com.linkedin.mxe.MetadataChangeProposal;

import lombok.Data;

@Data
public abstract class LineageEvent {
  private final String master;
  private final String appName;
  private final String appId;
  private final long time;

  public abstract List<MetadataChangeProposal> toMcps();

  protected String timeStr() {
    return new Date(getTime()).toInstant().toString();
  }
}