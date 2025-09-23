package com.linkedin.metadata.utils;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.MetadataChangeProposal;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityApiUtils {

  private EntityApiUtils() {}

  @Nonnull
  public static String toJsonAspect(@Nonnull final RecordTemplate aspectRecord) {
    return RecordUtils.toJsonString(aspectRecord);
  }

  public static RecordTemplate buildKeyAspect(
      @Nonnull EntityRegistry entityRegistry, @Nonnull final Urn urn) {
    final EntitySpec spec = entityRegistry.getEntitySpec(PegasusUtils.urnToEntityName(urn));
    final AspectSpec keySpec = spec.getKeyAspectSpec();
    return EntityKeyUtils.convertUrnToEntityKey(urn, keySpec);
  }

  public static <T extends RecordTemplate> MetadataChangeProposal buildMCP(
      Urn entityUrn, String aspectName, ChangeType changeType, @Nullable T aspect) {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(entityUrn);
    proposal.setChangeType(changeType);
    proposal.setEntityType(entityUrn.getEntityType());
    proposal.setAspectName(aspectName);
    if (aspect != null) {
      proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    }
    return proposal;
  }
}
