package com.linkedin.datahub.graphql.types.common.mappers.util;

import com.google.common.base.Ascii;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import lombok.AllArgsConstructor;


@AllArgsConstructor
public class UpdateMappingHelper {
  private final String entityName;

  public MetadataChangeProposal aspectToProposal(RecordTemplate aspect) {
    final MetadataChangeProposal metadataChangeProposal = new MetadataChangeProposal();
    metadataChangeProposal.setEntityType(entityName);
    metadataChangeProposal.setChangeType(ChangeType.UPSERT);
    metadataChangeProposal.setAspect(GenericAspectUtils.serializeAspect(aspect));
    String schemaName = aspect.schema().getName();
    // Schema returns PascalCase, but we want camelCase
    String aspectName = Ascii.toLowerCase(schemaName.charAt(0)) + schemaName.substring(1);
    metadataChangeProposal.setAspectName(aspectName);
    return metadataChangeProposal;
  }
}
