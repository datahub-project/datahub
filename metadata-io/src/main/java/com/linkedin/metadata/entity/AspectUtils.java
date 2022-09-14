package com.linkedin.metadata.entity;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AspectUtils {

  private AspectUtils() { }

  public static List<MetadataChangeProposal> getAdditionalChanges(
      @Nonnull MetadataChangeProposal metadataChangeProposal,
      @Nonnull EntityService entityService
  ) {
    // No additional changes for delete operation
    if (metadataChangeProposal.getChangeType() == ChangeType.DELETE) {
      return Collections.emptyList();
    }

    final Urn urn = EntityKeyUtils.getUrnFromProposal(metadataChangeProposal,
        entityService.getKeyAspectSpec(metadataChangeProposal.getEntityType()));

    return entityService.generateDefaultAspectsIfMissing(urn, ImmutableSet.of(metadataChangeProposal.getAspectName()))
        .stream()
        .map(entry -> getProposalFromAspect(entry.getKey(), entry.getValue(), metadataChangeProposal))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private static MetadataChangeProposal getProposalFromAspect(String aspectName, RecordTemplate aspect,
      MetadataChangeProposal original) {
    try {
      MetadataChangeProposal proposal = original.copy();
      GenericAspect genericAspect = GenericRecordUtils.serializeAspect(aspect);
      proposal.setAspect(genericAspect);
      proposal.setAspectName(aspectName);
      return proposal;
    } catch (CloneNotSupportedException e) {
      log.error("Issue while generating additional proposals corresponding to the input proposal", e);
    }
    return null;
  }
}
