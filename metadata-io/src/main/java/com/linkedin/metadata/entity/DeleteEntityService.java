package com.linkedin.metadata.entity;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;


@Slf4j
@RequiredArgsConstructor
public class DeleteEntityService {

  private final EntityService _entityService;

  /**
   * Utility method that processes the aspect of a given {@link RelatedEntity} instance that references a given
   * {@link Urn} removes said urn from the aspects and submits an MCP with the updated aspects.
   * @param urn           The urn to be found.
   * @param relatedEntity The entity to be modified.
   */
  public void deleteReference(Urn urn, RelatedEntity relatedEntity) {
    final Urn relatedUrn = UrnUtils.getUrn(relatedEntity.getUrn());
    final String relationType = relatedEntity.getRelationshipType();
    final String relatedEntityName = relatedUrn.getEntityType();

    log.debug("Processing related entity {} with relationship {}", relatedEntityName, relationType);

    final EntitySpec relatedEntitySpec = _entityService.getEntityRegistry().getEntitySpec(relatedEntityName);

    final java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> optionalAspectInfo =
        getAspectReferringTo(urn, relatedUrn, relationType, relatedEntitySpec);

    if (!optionalAspectInfo.isPresent()) {
      log.error("Unable to find aspect information that relates {} {} via relationship {}", urn, relatedUrn, relationType);
      return;
    }

    final String aspectName = optionalAspectInfo.get().getKey().getName();
    final Aspect aspect = optionalAspectInfo.get().getKey().getValue();
    final AspectSpec aspectSpec = optionalAspectInfo.get().getValue();
    final List<RelationshipFieldSpec> optionalRelationshipFieldSpec =
        aspectSpec.findRelationshipFor(relationType, urn.getEntityType());

    if (optionalRelationshipFieldSpec.isEmpty()) {
      log.error("Unable to find relationship spec information in {} that connects to {} via {}",
          aspectName, urn.getEntityType(), relationType);
      return;
    }

    for (RelationshipFieldSpec relationshipFieldSpec : optionalRelationshipFieldSpec) {
      final PathSpec path = relationshipFieldSpec.getPath();
      final Aspect updatedAspect = DeleteReferencesService.getAspectWithReferenceRemoved(urn.toString(), aspect,
          aspectSpec.getPegasusSchema(), path);

      final MetadataChangeProposal gmce = new MetadataChangeProposal();
      gmce.setEntityUrn(relatedUrn);
      gmce.setChangeType(ChangeType.UPSERT);
      gmce.setEntityType(relatedEntityName);
      gmce.setAspectName(aspectName);
      gmce.setAspect(GenericRecordUtils.serializeAspect(updatedAspect));

      final AuditStamp auditStamp = new AuditStamp().setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());
      final EntityService.IngestProposalResult ingestProposalResult = _entityService.ingestProposal(gmce, auditStamp);

      if (!ingestProposalResult.isDidUpdate()) {
        log.warn("Aspect update did not update metadata graph. Before {}, after: {}", aspect, updatedAspect);
      }
    }
  }

  /**
   * Utility method that attempts to find Aspect information as well as the associated path spec for a given urn that
   * has a relationship of type `relationType` to another urn
   * @param urn               The urn of the entity for which we want to find the aspect that relates the urn to
   *                          `relatedUrn`.
   * @param relatedUrn        The urn of the related entity in which we want to find the aspect that has a relationship
   *                          to `urn`.
   * @param relationType      The relationship type that details how the `urn` entity is related to `relatedUrn` entity.
   * @param relatedEntitySpec The entity spec of the related entity.
   * @return An {@link java.util.Optional} object containing the aspect content & respective spec that contains the
   * relationship between `urn` & `relatedUrn`.
   * @throws URISyntaxException
   */
  public java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> getAspectReferringTo(Urn urn, Urn relatedUrn,
      String relationType, @Nonnull EntitySpec relatedEntitySpec) {

    // Find which aspects are the candidates for the relationship we are looking for
    final Map<String, AspectSpec> aspectSpecs = relatedEntitySpec.getAspectSpecMap()
        .entrySet()
        .stream()
        .filter(aspectSpec -> !aspectSpec.getValue().findRelationshipFor(relationType, urn.getEntityType()).isEmpty())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    // FIXME: Can we not depend on entity service?
    final EntityResponse entityResponse;
    try {
      entityResponse = _entityService.getEntityV2(relatedUrn.getEntityType(),
          relatedUrn,
          aspectSpecs.keySet());
    } catch (URISyntaxException e) {
      log.error("Unable to retrieve entity data for relatedUrn " + relatedUrn, e);
      return java.util.Optional.empty();
    }

    // Find aspect which contains the relationship with the value we are looking for
    return entityResponse
        .getAspects().values()
        .stream()
        // Get aspects which contain the relationship field specs found above
        .filter(aspect -> aspectWithRelationship(urn, relationType, aspectSpecs, aspect))
        .map(aspect -> Pair.of(aspect, aspectSpecs.get(aspect.getName())))
        .findFirst();
  }

  /**
   * Utility method that specifies whether a given aspect has a relationship of type relationType to the given urn
   * @param urn
   * @param relationType
   * @param aspectSpecs
   * @param aspect
   * @return
   */
  private boolean aspectWithRelationship(Urn urn, String relationType, Map<String, AspectSpec> aspectSpecs, EnvelopedAspect aspect) {
    final AspectSpec aspectSpec = aspectSpecs.get(aspect.getName());
    final RecordTemplate recordTemplate =
        RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(),
            aspect.getValue().data());

    final Map<RelationshipFieldSpec, List<Object>> extractFields =
        FieldExtractor.extractFields(recordTemplate,
            aspectSpec.getRelationshipFieldSpecs());

    final List<RelationshipFieldSpec> relationshipFor =
        aspectSpec.findRelationshipFor(relationType, urn.getEntityType());

    return relationshipFor.stream().anyMatch(relationship -> !CollectionUtils.isEmpty(extractFields.get(relationship)));
  }
}
