package com.linkedin.metadata.entity;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTimeUtils;

@Slf4j
public class AspectUtils {

  private AspectUtils() {}

  public static final Set<ChangeType> SUPPORTED_TYPES =
      Set.of(ChangeType.UPSERT, ChangeType.CREATE, ChangeType.PATCH);

  public static List<MetadataChangeProposal> getAdditionalChanges(
      @Nonnull MetadataChangeProposal metadataChangeProposal,
      @Nonnull EntityService entityService,
      boolean onPrimaryKeyInsertOnly) {

    // No additional changes for unsupported operations
    if (!SUPPORTED_TYPES.contains(metadataChangeProposal.getChangeType())) {
      return Collections.emptyList();
    }

    final Urn urn =
        EntityKeyUtils.getUrnFromProposal(
            metadataChangeProposal,
            entityService.getKeyAspectSpec(metadataChangeProposal.getEntityType()));

    final Map<String, RecordTemplate> includedAspects;
    if (metadataChangeProposal.getChangeType() != ChangeType.PATCH) {
      RecordTemplate aspectRecord =
          GenericRecordUtils.deserializeAspect(
              metadataChangeProposal.getAspect().getValue(),
              metadataChangeProposal.getAspect().getContentType(),
              entityService
                  .getEntityRegistry()
                  .getEntitySpec(urn.getEntityType())
                  .getAspectSpec(metadataChangeProposal.getAspectName()));
      includedAspects = ImmutableMap.of(metadataChangeProposal.getAspectName(), aspectRecord);
    } else {
      includedAspects = ImmutableMap.of();
    }

    if (onPrimaryKeyInsertOnly) {
      return entityService
          .generateDefaultAspectsOnFirstWrite(urn, includedAspects)
          .getValue()
          .stream()
          .map(
              entry ->
                  getProposalFromAspect(entry.getKey(), entry.getValue(), metadataChangeProposal))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    } else {
      return entityService.generateDefaultAspectsIfMissing(urn, includedAspects).stream()
          .map(
              entry ->
                  getProposalFromAspect(entry.getKey(), entry.getValue(), metadataChangeProposal))
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
    }
  }

  public static List<MetadataChangeProposal> getAdditionalChanges(
      @Nonnull MetadataChangeProposal metadataChangeProposal,
      @Nonnull EntityService entityService) {

    return getAdditionalChanges(metadataChangeProposal, entityService, false);
  }

  public static Map<Urn, Aspect> batchGetLatestAspect(
      String entity,
      Set<Urn> urns,
      String aspectName,
      EntityClient entityClient,
      Authentication authentication)
      throws Exception {
    final Map<Urn, EntityResponse> gmsResponse =
        entityClient.batchGetV2(entity, urns, ImmutableSet.of(aspectName), authentication);
    final Map<Urn, Aspect> finalResult = new HashMap<>();
    for (Urn urn : urns) {
      EntityResponse response = gmsResponse.get(urn);
      if (response != null && response.getAspects().containsKey(aspectName)) {
        finalResult.put(urn, response.getAspects().get(aspectName).getValue());
      }
    }
    return finalResult;
  }

  private static MetadataChangeProposal getProposalFromAspect(
      String aspectName, RecordTemplate aspect, MetadataChangeProposal original) {
    MetadataChangeProposal proposal = new MetadataChangeProposal();
    GenericAspect genericAspect = GenericRecordUtils.serializeAspect(aspect);
    // Set net new fields
    proposal.setAspect(genericAspect);
    proposal.setAspectName(aspectName);

    // Set fields determined from original
    // Additional changes should never be set as PATCH, if a PATCH is coming across it should be an
    // UPSERT
    proposal.setChangeType(original.getChangeType());
    if (ChangeType.PATCH.equals(proposal.getChangeType())) {
      proposal.setChangeType(ChangeType.UPSERT);
    }

    if (original.getSystemMetadata() != null) {
      proposal.setSystemMetadata(original.getSystemMetadata());
    }
    if (original.getEntityUrn() != null) {
      proposal.setEntityUrn(original.getEntityUrn());
    }
    if (original.getEntityKeyAspect() != null) {
      proposal.setEntityKeyAspect(original.getEntityKeyAspect());
    }
    if (original.getAuditHeader() != null) {
      proposal.setAuditHeader(original.getAuditHeader());
    }

    proposal.setEntityType(original.getEntityType());

    return proposal;
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

  public static AuditStamp getAuditStamp(Urn actor) {
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(DateTimeUtils.currentTimeMillis());
    auditStamp.setActor(actor);
    return auditStamp;
  }
}
