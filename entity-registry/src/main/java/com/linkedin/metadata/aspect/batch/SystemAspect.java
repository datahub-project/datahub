package com.linkedin.metadata.aspect.batch;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import java.sql.Timestamp;

/**
 * An aspect along with system metadata and creation timestamp. Represents an aspect as stored in
 * primary storage.
 */
public interface SystemAspect {
  Urn getUrn();

  String getAspectName();

  long getVersion();

  RecordTemplate getRecordTemplate(EntityRegistry entityRegistry);

  SystemMetadata getSystemMetadata();

  Timestamp getCreatedOn();
}
