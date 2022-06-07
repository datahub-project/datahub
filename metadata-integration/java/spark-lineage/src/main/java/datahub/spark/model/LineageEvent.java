package datahub.spark.model;

import java.util.Date;
import java.util.List;

import datahub.event.MetadataChangeProposalWrapper;
import lombok.Data;

@Data
public abstract class LineageEvent {
  private final String master;
  private final String appName;
  private final String appId;
  private final long time;

  public abstract List<MetadataChangeProposalWrapper> asMetadataEvents();

  public String timeStr() {
    return new Date(getTime()).toInstant().toString();
  }
}