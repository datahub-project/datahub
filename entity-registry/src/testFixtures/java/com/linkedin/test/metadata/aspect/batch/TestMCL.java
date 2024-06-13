package com.linkedin.test.metadata.aspect.batch;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.MCLItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.MetadataChangeLog;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Getter;

@Builder(toBuilder = true)
@Getter
public class TestMCL implements MCLItem {
  private Urn urn;
  private ChangeType changeType;
  private MetadataChangeLog metadataChangeLog;
  private RecordTemplate previousRecordTemplate;
  private RecordTemplate recordTemplate;
  private EntitySpec entitySpec;
  private AspectSpec aspectSpec;
  private AuditStamp auditStamp;

  @Nonnull
  @Override
  public String getAspectName() {
    return getAspectSpec().getName();
  }
}
