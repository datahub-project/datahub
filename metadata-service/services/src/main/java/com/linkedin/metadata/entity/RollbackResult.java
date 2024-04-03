package com.linkedin.metadata.entity;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.SystemMetadata;
import lombok.Value;

@Value
public class RollbackResult {
  public Urn urn;
  public String entityName;
  public String aspectName;
  public RecordTemplate oldValue;
  public RecordTemplate newValue;
  public SystemMetadata oldSystemMetadata;
  public SystemMetadata newSystemMetadata;
  public ChangeType changeType;
  public Boolean keyAffected;
  public Integer additionalRowsAffected;
}
