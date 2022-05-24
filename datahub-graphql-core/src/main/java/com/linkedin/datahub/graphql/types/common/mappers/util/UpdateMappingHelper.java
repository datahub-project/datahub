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
