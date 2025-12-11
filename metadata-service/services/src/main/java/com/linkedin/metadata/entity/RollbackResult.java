/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.entity;

import static com.linkedin.metadata.utils.PegasusUtils.constructMCL;
import static com.linkedin.metadata.utils.PegasusUtils.urnToEntityName;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.mxe.MetadataChangeLog;
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

  public boolean isNoOp() {
    return oldValue == newValue;
  }

  public MetadataChangeLog toMCL(AuditStamp auditStamp) {
    return constructMCL(
        null,
        urnToEntityName(urn),
        urn,
        changeType,
        aspectName,
        auditStamp,
        newValue,
        newSystemMetadata,
        oldValue,
        oldSystemMetadata);
  }
}
