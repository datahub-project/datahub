package com.linkedin.metadata.entity;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
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
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.graph.RelatedEntity;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RelatedAspect;
import com.linkedin.metadata.run.RelatedAspectArray;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;

import static com.linkedin.metadata.search.utils.QueryUtils.*;


@Slf4j
@RequiredArgsConstructor
public class DeleteEntityService {

  private final EntityService _entityService;
  private final GraphService _graphService;

  private static final Integer ELASTIC_BATCH_DELETE_SLEEP_SEC = 5;

  /**
   * Public endpoint that deletes references to a given urn across DataHub's metadata graph. This is the entrypoint for
   * addressing dangling pointers whenever a user deletes some entity.
   *
   * @param urn     The urn for which to delete references in DataHub's metadata graph.
   * @param dryRun  Specifies if the delete logic should be executed to conclusion or if the caller simply wants a
   *                preview of the response.
   * @return  A {@link DeleteReferencesResponse} instance detailing the response of deleting references to the provided
   * urn.
   */
  public DeleteReferencesResponse deleteReferencesTo(final Urn urn, final boolean dryRun) {
    final DeleteReferencesResponse result = new DeleteReferencesResponse();
    RelatedEntitiesResult relatedEntities =
        _graphService.findRelatedEntities(null, newFilter("urn", urn.toString()), null,
            EMPTY_FILTER,
            ImmutableList.of(),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 10000);

    final List<RelatedAspect> relatedAspects = relatedEntities.getEntities().stream().map(relatedEntity -> {
          final Urn relatedUrn = UrnUtils.getUrn(relatedEntity.getUrn());
          final String relationType = relatedEntity.getRelationshipType();
          final String relatedEntityName = relatedUrn.getEntityType();
          final EntitySpec relatedEntitySpec = _entityService.getEntityRegistry().getEntitySpec(relatedEntityName);

          return getAspectReferringTo(urn, relatedUrn, relationType, relatedEntitySpec).map(pair -> {
            final RelatedAspect relatedAspect = new RelatedAspect();
            relatedAspect.setEntity(relatedUrn);
            relatedAspect.setRelationship(relationType);
            relatedAspect.setAspect(pair.getSecond().getName());
            return relatedAspect;
          });
        }).filter(java.util.Optional::isPresent)
        .map(java.util.Optional::get)
        .limit(10).collect(Collectors.toList());

    result.setRelatedAspects(new RelatedAspectArray(relatedAspects));
    result.setTotal(relatedEntities.getTotal());

    if (dryRun) {
      return result;
    }

    int processedEntities = 0;

    // Delete first 10k
    relatedEntities.getEntities().forEach(entity -> deleteReference(urn, entity));

    processedEntities += relatedEntities.getEntities().size();

    // Delete until less than 10k are left
    while (relatedEntities.getCount() > 10000) {
      log.info("Processing batch {} of {} aspects", processedEntities, relatedEntities.getTotal());
      sleep(ELASTIC_BATCH_DELETE_SLEEP_SEC);
      relatedEntities = _graphService.findRelatedEntities(null, newFilter("urn", urn.toString()),
          null, EMPTY_FILTER, ImmutableList.of(),
          newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING), 0, 10000);
      relatedEntities.getEntities().forEach(entity -> deleteReference(urn, entity));
      processedEntities += relatedEntities.getEntities().size();
    }

    return result;
  }

  /**
   * Utility method to sleep the thread.
   * @param seconds The number of seconds to sleep.
   */
  private void sleep(final Integer seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      log.error("Interrupted sleep", e);
    }
  }

  /**
   * Processes an aspect of a given {@link RelatedEntity} instance that references a given {@link Urn}, removes said
   * urn from the aspects and submits an MCP with the updated aspects.
   * @param urn           The urn to be found.
   * @param relatedEntity The entity to be modified.
   */
  private void deleteReference(final Urn urn, final RelatedEntity relatedEntity) {
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
   */
  public java.util.Optional<Pair<EnvelopedAspect, AspectSpec>> getAspectReferringTo(final Urn urn, final Urn relatedUrn,
      final String relationType, final @Nonnull EntitySpec relatedEntitySpec) {

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
        .filter(aspect -> aspectWithRelationshipTo(urn.getEntityType(), relationType, aspectSpecs, aspect))
        .map(aspect -> Pair.of(aspect, aspectSpecs.get(aspect.getName())))
        .findFirst();
  }

  /**
   * Utility method that determines whether a given aspect has a relationship of type relationType to a given entity type.
   * @param entityType    The name of the entity the method checks against.
   * @param relationType  The name of the relationship to search for.
   * @param aspectSpecs   A dictionary of relationship types to aspect specs in which to search for a concrete
   *                      relationship with name=relationType and that targets the entityType passed by parameter.
   * @param aspect        The aspect in which to search for the relationship.
   * @return {@code True} if the aspect has a relationship with the intended conditions, {@code False} otherwise.
   */
  private boolean aspectWithRelationshipTo(final String entityType, final String relationType,
      final Map<String, AspectSpec> aspectSpecs, final EnvelopedAspect aspect) {

    final AspectSpec aspectSpec = aspectSpecs.get(aspect.getName());
    if (aspectSpec == null) {
      return false;
    }
    final RecordTemplate recordTemplate =
        RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(),
            aspect.getValue().data());

    final Map<RelationshipFieldSpec, List<Object>> extractFields =
        FieldExtractor.extractFields(recordTemplate,
            aspectSpec.getRelationshipFieldSpecs());

    final List<RelationshipFieldSpec> relationshipFor =
        aspectSpec.findRelationshipFor(relationType, entityType);

    return relationshipFor.stream().anyMatch(relationship -> !CollectionUtils.isEmpty(extractFields.get(relationship)));
  }
}
