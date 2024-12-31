package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AspectUtils {

  private AspectUtils() {}

  public static Map<Urn, Aspect> batchGetLatestAspect(
      @Nonnull OperationContext opContext,
      String entity,
      Set<Urn> urns,
      String aspectName,
      EntityClient entityClient)
      throws Exception {
    final Map<Urn, EntityResponse> gmsResponse =
        entityClient.batchGetV2(opContext, entity, urns, ImmutableSet.of(aspectName));
    final Map<Urn, Aspect> finalResult = new HashMap<>();
    for (Urn urn : urns) {
      EntityResponse response = gmsResponse.get(urn);
      if (response != null && response.getAspects().containsKey(aspectName)) {
        finalResult.put(urn, response.getAspects().get(aspectName).getValue());
      }
    }
    return finalResult;
  }

  public static MetadataChangeProposal buildMetadataChangeProposal(
      @Nonnull Urn urn, @Nonnull String aspectName, @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  public static MetadataChangeProposal buildMetadataChangeProposal(
      @Nonnull String entityType,
      @Nonnull RecordTemplate keyAspect,
      @Nonnull String aspectName,
      @Nonnull RecordTemplate aspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityType(entityType);
    proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(keyAspect));
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    proposal.setChangeType(ChangeType.UPSERT);
    return proposal;
  }

  public static AspectSpec validateAspect(MetadataChangeLog mcl, EntitySpec entitySpec) {
    if (!mcl.hasAspectName()
        || (!ChangeType.DELETE.equals(mcl.getChangeType()) && !mcl.hasAspect())) {
      throw new UnsupportedOperationException(
          String.format(
              "Aspect and aspect name is required for create and update operations. changeType: %s entityName: %s hasAspectName: %s hasAspect: %s",
              mcl.getChangeType(), entitySpec.getName(), mcl.hasAspectName(), mcl.hasAspect()));
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(mcl.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Unknown aspect %s for entity %s", mcl.getAspectName(), mcl.getEntityType()));
    }

    return aspectSpec;
  }

  public static AspectSpec validateAspect(MetadataChangeProposal mcp, EntitySpec entitySpec) {
    if (!mcp.hasAspectName() || !mcp.hasAspect()) {
      throw new UnsupportedOperationException(
          "Aspect and aspect name is required for create and update operations");
    }

    AspectSpec aspectSpec = entitySpec.getAspectSpec(mcp.getAspectName());

    if (aspectSpec == null) {
      throw new RuntimeException(
          String.format(
              "Unknown aspect %s for entity %s", mcp.getAspectName(), mcp.getEntityType()));
    }

    return aspectSpec;
  }
}
