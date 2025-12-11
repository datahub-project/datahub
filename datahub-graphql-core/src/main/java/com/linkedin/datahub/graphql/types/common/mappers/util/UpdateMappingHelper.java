/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class UpdateMappingHelper {
  private final String entityName;

  public MetadataChangeProposal aspectToProposal(RecordTemplate aspect, String aspectName) {
    final MetadataChangeProposal metadataChangeProposal = new MetadataChangeProposal();
    metadataChangeProposal.setEntityType(entityName);
    metadataChangeProposal.setChangeType(ChangeType.UPSERT);
    metadataChangeProposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    metadataChangeProposal.setAspectName(aspectName);
    return metadataChangeProposal;
  }
}
