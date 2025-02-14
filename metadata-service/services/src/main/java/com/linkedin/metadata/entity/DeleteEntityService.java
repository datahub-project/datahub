package com.linkedin.metadata.entity;

import static com.linkedin.metadata.search.utils.QueryUtils.*;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.schema.PathSpec;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.models.graph.RelatedEntity;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.RelatedEntitiesResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.RelationshipFieldSpec;
import com.linkedin.metadata.models.extractor.FieldExtractor;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RelatedAspect;
import com.linkedin.metadata.run.RelatedAspectArray;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteEntityService {

  private final EntityService<?> _entityService;
  private final GraphService _graphService;
  private final EntitySearchService _searchService;

  private static final Integer ELASTIC_BATCH_DELETE_SLEEP_SEC = 5;
  private static final Integer BATCH_SIZE = 1000;

  /**
   * Public endpoint that deletes references to a given urn across DataHub's metadata graph. This is
   * the entrypoint for addressing dangling pointers whenever a user deletes some entity.
   *
   * @param urn The urn for which to delete references in DataHub's metadata graph.
   * @param dryRun Specifies if the delete logic should be executed to conclusion or if the caller
   *     simply wants a preview of the response.
   * @return A {@link DeleteReferencesResponse} instance detailing the response of deleting
   *     references to the provided urn.
   */
  public DeleteReferencesResponse deleteReferencesTo(
      @Nonnull OperationContext opContext, final Urn urn, final boolean dryRun) {
    // TODO: update DeleteReferencesResponse to have searchAspects and provide more helpful comment
    // in CLI
    final DeleteReferencesResponse result = new DeleteReferencesResponse();

    // Delete references for entities referencing the deleted urn with searchables.
    // Only works for Form deletion for now
    int totalSearchAssetCount = deleteSearchReferences(opContext, urn, dryRun);

    RelatedEntitiesResult relatedEntities =
        _graphService.findRelatedEntities(
            opContext,
            null,
            newFilter("urn", urn.toString()),
            null,
            EMPTY_FILTER,
            ImmutableList.of(),
            newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
            0,
            10000);

    final List<RelatedAspect> relatedAspects =
        relatedEntities.getEntities().stream()
            .flatMap(
                relatedEntity ->
                    getRelatedAspectStream(
                        opContext,
                        urn,
                        UrnUtils.getUrn(relatedEntity.getUrn()),
                        relatedEntity.getRelationshipType()))
            .limit(10)
            .collect(Collectors.toList());

    result.setRelatedAspects(new RelatedAspectArray(relatedAspects));
    result.setTotal(relatedEntities.getTotal() + totalSearchAssetCount);

    if (dryRun) {
      return result;
    }

    for (int processedEntities = 0;
        processedEntities < relatedEntities.getTotal();
        processedEntities += relatedEntities.getCount()) {
      log.info("Processing batch {} of {} aspects", processedEntities, relatedEntities.getTotal());
      relatedEntities.getEntities().forEach(entity -> deleteReference(opContext, urn, entity));
      if (processedEntities + relatedEntities.getEntities().size() < relatedEntities.getTotal()) {
        sleep(ELASTIC_BATCH_DELETE_SLEEP_SEC);
        relatedEntities =
            _graphService.findRelatedEntities(
                opContext,
                null,
                newFilter("urn", urn.toString()),
                null,
                EMPTY_FILTER,
                ImmutableList.of(),
                newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
                0,
                10000);
      }
    }

    return result;
  }

  /**
   * Gets a stream of relatedAspects Pojos (high-level, trimmed information) that relate an entity
   * with urn `urn` to another entity of urn `relatedUrn` via a concrete relationship type. Used to
   * give users of this API a summary of what aspects are related to a given urn and how.
   *
   * @param urn The identifier of the source entity.
   * @param relatedUrn The identifier of the destination entity.
   * @param relationshipType The name of the relationship type that links urn to relatedUrn.
   * @return A stream of {@link RelatedAspect} instances that have the relationship from urn to
   *     relatedUrn.
   */
  private Stream<RelatedAspect> getRelatedAspectStream(
      @Nonnull OperationContext opContext, Urn urn, Urn relatedUrn, String relationshipType) {
    return getAspects(opContext, urn, relatedUrn, relationshipType)
        .map(
            enrichedAspect -> {
              final RelatedAspect relatedAspect = new RelatedAspect();
              relatedAspect.setEntity(relatedUrn);
              relatedAspect.setRelationship(relationshipType);
              relatedAspect.setAspect(enrichedAspect.getName());
              return relatedAspect;
            });
  }

  /**
   * Gets a stream of Enriched Aspect Pojos (Aspect + aspect spec tuple) that relate an entity with
   * urn `urn` to another entity of urn `relatedUrn` via a concrete relationship type.
   *
   * @param urn The identifier of the source entity.
   * @param relatedUrn The identifier of the destination entity.
   * @param relationshipType The name of the relationship type that links urn to relatedUrn.
   * @return A stream of {@link EnrichedAspect} instances that have the relationship from urn to
   *     relatedUrn.
   */
  private Stream<EnrichedAspect> getAspects(
      @Nonnull OperationContext opContext, Urn urn, Urn relatedUrn, String relationshipType) {
    final String relatedEntityName = relatedUrn.getEntityType();
    final EntitySpec relatedEntitySpec =
        opContext.getEntityRegistry().getEntitySpec(relatedEntityName);
    final Map<String, AspectSpec> aspectSpecs =
        getAspectSpecsReferringTo(urn.getEntityType(), relationshipType, relatedEntitySpec);

    // If we have an empty map it means that we have a graph edge that points to some aspect spec
    // that we can't find in
    // the entity registry. It would be a corrupted edge in the graph index or backwards
    // incompatible change in the
    // entity registry (I.e: deleting the aspect from the metadata model without being consistent in
    // the graph index).
    if (aspectSpecs.isEmpty()) {
      log.error(
          "Unable to find any aspect spec that has a {} relationship to {} entities. This means that the entity "
              + "registry does not have relationships that the graph index has stored.",
          relationshipType,
          relatedEntityName);
      handleError(
          new DeleteEntityServiceError(
              "Unable to find aspect spec in entity registry",
              DeleteEntityServiceErrorReason.ENTITY_REGISTRY_SPEC_NOT_FOUND,
              ImmutableMap.of(
                  "relatedEntityName",
                  relatedEntityName,
                  "relationshipType",
                  relationshipType,
                  "relatedEntitySpec",
                  relatedEntitySpec)));
      return Stream.empty();
    }

    final List<EnvelopedAspect> aspectList =
        getAspectsReferringTo(opContext, relatedUrn, aspectSpecs).collect(Collectors.toList());

    // If we have an empty list it means that we have a graph edge that points to some aspect that
    // we can't find in the
    // entity service. It would be a corrupted edge in the graph index or corrupted record in the
    // entity DB.
    if (aspectList.isEmpty()) {
      log.error(
          "Unable to find an aspect instance that relates {} {} via relationship {} in the entity service. "
              + "This is potentially a lack of consistency between the graph and entity DBs.",
          urn,
          relatedUrn,
          relationshipType);
      handleError(
          new DeleteEntityServiceError(
              "Unable to find aspect instance in entity service",
              DeleteEntityServiceErrorReason.ENTITY_SERVICE_ASPECT_NOT_FOUND,
              ImmutableMap.of(
                  "urn",
                  urn,
                  "relatedUrn",
                  relatedUrn,
                  "relationship",
                  relationshipType,
                  "aspectSpecs",
                  aspectSpecs)));
      return Stream.empty();
    }

    return aspectList.stream()
        .filter(
            envelopedAspect ->
                hasRelationshipInstanceTo(
                    envelopedAspect.getValue(),
                    urn.getEntityType(),
                    relationshipType,
                    aspectSpecs.get(envelopedAspect.getName())))
        .map(
            envelopedAspect ->
                new EnrichedAspect(
                    envelopedAspect.getName(),
                    envelopedAspect.getValue(),
                    aspectSpecs.get(envelopedAspect.getName())));
  }

  /**
   * Utility method to sleep the thread.
   *
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
   * Processes an aspect of a given {@link RelatedEntity} instance that references a given {@link
   * Urn}, removes said urn from the aspects and submits an MCP with the updated aspects.
   *
   * @param urn The urn to be found.
   * @param relatedEntity The entity to be modified.
   */
  private void deleteReference(
      @Nonnull OperationContext opContext, final Urn urn, final RelatedEntity relatedEntity) {
    final Urn relatedUrn = UrnUtils.getUrn(relatedEntity.getUrn());
    final String relationshipType = relatedEntity.getRelationshipType();
    getAspects(opContext, urn, relatedUrn, relationshipType)
        .forEach(
            enrichedAspect -> {
              final String aspectName = enrichedAspect.getName();
              final Aspect aspect = enrichedAspect.getAspect();
              final AspectSpec aspectSpec = enrichedAspect.getSpec();

              final AtomicReference<Aspect> updatedAspect;
              try {
                updatedAspect = new AtomicReference<>(aspect.copy());
              } catch (CloneNotSupportedException e) {
                log.error("Failed to clone aspect {}", aspect);
                handleError(
                    new DeleteEntityServiceError(
                        "Failed to clone aspect",
                        DeleteEntityServiceErrorReason.CLONE_FAILED,
                        ImmutableMap.of("aspect", aspect)));
                return;
              }

              aspectSpec.getRelationshipFieldSpecs().stream()
                  .filter(
                      relationshipFieldSpec ->
                          relationshipFieldSpec
                              .getRelationshipAnnotation()
                              .getName()
                              .equals(relationshipType))
                  .forEach(
                      relationshipFieldSpec -> {
                        final PathSpec path = relationshipFieldSpec.getPath();
                        updatedAspect.set(
                            DeleteEntityUtils.getAspectWithReferenceRemoved(
                                urn.toString(),
                                updatedAspect.get(),
                                aspectSpec.getPegasusSchema(),
                                path));
                      });

              // If there has been an update, then we produce an MCE.
              if (!aspect.equals(updatedAspect.get())) {
                if (updatedAspect.get() == null) {
                  // Then we should remove the aspect.
                  deleteAspect(opContext, relatedUrn, aspectName, aspect);
                } else {
                  // Then we should update the aspect.
                  updateAspect(opContext, relatedUrn, aspectName, aspect, updatedAspect.get());
                }
              }
            });
  }

  /**
   * Delete an existing aspect for an urn.
   *
   * @param urn the urn of the entity to remove the aspect for
   * @param aspectName the aspect to remove
   * @param prevAspect the old value for the aspect
   */
  private void deleteAspect(
      @Nonnull OperationContext opContext, Urn urn, String aspectName, RecordTemplate prevAspect) {
    final Optional<RollbackResult> rollbackResult =
        _entityService.deleteAspect(opContext, urn.toString(), aspectName, new HashMap<>(), true);
    if (rollbackResult.isEmpty() || rollbackResult.get().getNewValue() != null) {
      log.error(
          "Failed to delete aspect with references. Before {}, after: null, please check GMS logs"
              + " logs for more information",
          prevAspect);
      handleError(
          new DeleteEntityServiceError(
              "Failed to ingest new aspect",
              DeleteEntityServiceErrorReason.ASPECT_DELETE_FAILED,
              ImmutableMap.of("urn", urn, "aspectName", aspectName)));
    }
  }

  /**
   * Update an aspect for an urn.
   *
   * @param urn the urn of the entity to remove the aspect for
   * @param aspectName the aspect to remove
   * @param prevAspect the old value for the aspect
   * @param newAspect the new value for the aspect
   */
  private void updateAspect(
      @Nonnull OperationContext opContext,
      Urn urn,
      String aspectName,
      RecordTemplate prevAspect,
      RecordTemplate newAspect) {
    final MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(aspectName);
    proposal.setAspect(GenericRecordUtils.serializeAspect(newAspect));

    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());
    final IngestResult ingestProposalResult =
        _entityService.ingestProposal(opContext, proposal, auditStamp, false);

    if (ingestProposalResult != null && !ingestProposalResult.isSqlCommitted()) {
      log.error(
          "Failed to ingest aspect with references removed. Before {}, after: {}, please check MCP processor"
              + " logs for more information",
          prevAspect,
          newAspect);
      handleError(
          new DeleteEntityServiceError(
              "Failed to ingest new aspect",
              DeleteEntityServiceErrorReason.MCP_PROCESSOR_FAILED,
              ImmutableMap.of("proposal", proposal)));
    }
  }

  /**
   * Utility method that attempts to find Aspect information as well as the associated path spec for
   * a given urn that has a relationship of type `relationType` to another urn.
   *
   * @param relatedUrn The urn of the related entity in which we want to find the aspect that has a
   *     relationship to `urn`.
   * @param aspectSpecs The entity spec of the related entity.
   * @return A {@link Stream} of {@link EnvelopedAspect} instances that contain relationships
   *     between `urn` & `relatedUrn`.
   */
  private Stream<EnvelopedAspect> getAspectsReferringTo(
      @Nonnull OperationContext opContext,
      final Urn relatedUrn,
      final Map<String, AspectSpec> aspectSpecs) {

    // FIXME: Can we not depend on entity service?
    final EntityResponse entityResponse;
    try {
      entityResponse =
          _entityService.getEntityV2(
              opContext, relatedUrn.getEntityType(), relatedUrn, aspectSpecs.keySet());
    } catch (URISyntaxException e) {
      log.error("Unable to retrieve entity data for relatedUrn " + relatedUrn, e);
      return Stream.empty();
    }
    // Find aspect which contains the relationship with the value we are looking for
    return entityResponse.getAspects().values().stream()
        // Get aspects which contain the relationship field specs found above
        .filter(Objects::nonNull)
        .filter(aspect -> aspectSpecs.containsKey(aspect.getName()));
  }

  /**
   * Utility method that determines whether a given aspect has an instance of a relationship of type
   * relationType to a given entity type.
   *
   * @param aspect The aspect in which to search for the relationship.
   * @param entityType The name of the entity the method checks against.
   * @param relationType The name of the relationship to search for.
   * @param aspectSpec The aspect spec in which to search for a concrete relationship with
   *     name=relationType and that targets the entityType passed by parameter.
   * @return {@code True} if the aspect has a relationship with the intended conditions, {@code
   *     False} otherwise.
   */
  private boolean hasRelationshipInstanceTo(
      final Aspect aspect,
      final String entityType,
      final String relationType,
      final AspectSpec aspectSpec) {

    final RecordTemplate recordTemplate =
        RecordUtils.toRecordTemplate(aspectSpec.getDataTemplateClass(), aspect.data());

    final Map<RelationshipFieldSpec, List<Object>> extractFields =
        FieldExtractor.extractFields(recordTemplate, aspectSpec.getRelationshipFieldSpecs());

    // Is there is any instance of the relationship specs defined in the aspect's spec extracted
    // from the
    // aspect record instance?
    return findRelationshipFor(aspectSpec, relationType, entityType)
        .map(extractFields::get)
        .filter(Objects::nonNull)
        .anyMatch(list -> !list.isEmpty());
  }

  /**
   * Computes the set of aspect specs of an entity that contain a relationship of a given name to a
   * specific entity type.
   *
   * @param relatedEntityType The name of the entity.
   * @param relationshipType The name of the relationship.
   * @param entitySpec The entity spec from which to retrieve the aspect specs, if any.
   * @return A filtered dictionary of aspect name to aspect specs containing only aspects that have
   *     a relationship of name relationshipType to the given relatedEntityType.
   */
  private Map<String, AspectSpec> getAspectSpecsReferringTo(
      final String relatedEntityType, final String relationshipType, final EntitySpec entitySpec) {
    return entitySpec.getAspectSpecMap().entrySet().stream()
        .filter(
            entry ->
                findRelationshipFor(entry.getValue(), relationshipType, relatedEntityType)
                    .findAny()
                    .isPresent())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  /**
   * Utility method to find the relationship specs within an AspectSpec with name relationshipName
   * and which has relatedEntity name as a valid destination type.
   *
   * @param spec The aspect spec from which to extract relationship field specs.
   * @param relationshipType The name of the relationship to find.
   * @param entityType The name of the entity type (i.e: dataset, chart, usergroup, etc...) which
   *     the relationship is valid for.
   * @return The list of relationship field specs which match the criteria.
   */
  private Stream<RelationshipFieldSpec> findRelationshipFor(
      final AspectSpec spec, final String relationshipType, final String entityType) {
    return spec.getRelationshipFieldSpecs().stream()
        .filter(
            relationship ->
                relationship.getRelationshipName().equals(relationshipType)
                    && relationship.getValidDestinationTypes().contains(entityType));
  }

  /**
   * Entrypoint to handle the various errors that may occur during the execution of the delete
   * entity service.
   *
   * @param error The error instance that provides context on what issue occured.
   */
  private void handleError(final DeleteEntityServiceError error) {
    // NO-OP for now.
  }

  private static class AssetScrollResult {
    String scrollId;
    List<Urn> assets;
    int totalAssetCount;
  }

  /**
   * Method that will find references through the search index and clean aspects up. Right now this
   * only works for deleting Forms entities. Later, we need to extend this for all searchables.
   */
  private int deleteSearchReferences(
      @Nonnull OperationContext opContext, @Nonnull final Urn deletedUrn, final boolean dryRun) {
    int totalAssetCount = 0;
    String scrollId = null;
    do {
      AssetScrollResult assetScrollResult =
          getAssetsReferencingUrn(opContext, deletedUrn, scrollId, dryRun);
      List<Urn> assetsReferencingUrn = assetScrollResult.assets;
      totalAssetCount += assetScrollResult.totalAssetCount;
      // if it's a dry run, exit early and stop looping over assets
      scrollId = dryRun ? null : assetScrollResult.scrollId;
      if (!dryRun) {
        assetsReferencingUrn.forEach(
            assetUrn -> {
              List<MetadataChangeProposal> mcps =
                  deleteSearchReferencesForAsset(opContext, assetUrn, deletedUrn);
              mcps.forEach(
                  mcp -> _entityService.ingestProposal(opContext, mcp, createAuditStamp(), true));
            });
      }
    } while (scrollId != null);
    return totalAssetCount;
  }

  /**
   * Get all of the asset urns that reference the deleted urn This is hardcoded for forms right now,
   * we will use a more generic field to work with all searchables later.
   *
   * <p>TODO: Use the new referencedUrns index field once that exists
   */
  private AssetScrollResult getAssetsReferencingUrn(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn deletedUrn,
      @Nullable String scrollId,
      final boolean dryRun) {
    AssetScrollResult result = new AssetScrollResult();
    result.scrollId = null;
    result.totalAssetCount = 0;
    result.assets = new ArrayList<>();

    if (deletedUrn.getEntityType().equals("form")) {
      Filter filter = DeleteEntityUtils.getFilterForFormDeletion(deletedUrn);
      List<String> entityNames = DeleteEntityUtils.getEntityNamesForFormDeletion();
      return scrollForAssets(opContext, result, filter, entityNames, scrollId, dryRun);
    }
    if (deletedUrn.getEntityType().equals("structuredProperty")) {
      Filter filter = DeleteEntityUtils.getFilterForStructuredPropertyDeletion(deletedUrn);
      List<String> entityNames = DeleteEntityUtils.getEntityNamesForStructuredPropertyDeletion();
      return scrollForAssets(opContext, result, filter, entityNames, scrollId, dryRun);
    }
    return result;
  }

  private AssetScrollResult scrollForAssets(
      @Nonnull OperationContext opContext,
      AssetScrollResult result,
      final @Nullable Filter filter,
      final @Nonnull List<String> entityNames,
      @Nullable String scrollId,
      final boolean dryRun) {
    ScrollResult scrollResult =
        _searchService.structuredScroll(
            opContext,
            entityNames,
            "*",
            filter,
            null,
            scrollId,
            "5m",
            dryRun ? 1 : BATCH_SIZE); // need to pass in 1 for count otherwise get index error
    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().size() == 0) {
      return result;
    }
    result.scrollId = scrollResult.getScrollId();
    result.totalAssetCount = scrollResult.getNumEntities();
    result.assets =
        scrollResult.getEntities().stream()
            .map(SearchEntity::getEntity)
            .collect(Collectors.toList());
    return result;
  }

  /** Get all the aspects that need updating, then loop over and update them */
  private List<MetadataChangeProposal> deleteSearchReferencesForAsset(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assetUrn,
      @Nonnull final Urn deletedUrn) {
    // delete entities that should be deleted first
    if (shouldDeleteAssetReferencingUrn(assetUrn, deletedUrn)) {
      _entityService.deleteUrn(opContext, assetUrn);
    }

    List<MetadataChangeProposal> mcps = new ArrayList<>();
    List<String> aspectsToUpdate = getAspectsToUpdate(deletedUrn, assetUrn);
    aspectsToUpdate.forEach(
        aspectName -> {
          try {
            MetadataChangeProposal mcp =
                updateAspectForSearchReference(opContext, assetUrn, deletedUrn, aspectName);
            if (mcp != null) {
              mcps.add(mcp);
            }
          } catch (Exception e) {
            log.error(
                String.format(
                    "Error trying to update aspect %s for asset %s when deleting %s",
                    aspectName, assetUrn, deletedUrn),
                e);
          }
        });
    return mcps;
  }

  /**
   * Get the names of all the aspects that need to be updated when the deleted urn is removed.
   *
   * <p>TODO: extend this to support other types of deletes and be more dynamic depending on aspects
   * that the asset has
   */
  private List<String> getAspectsToUpdate(
      @Nonnull final Urn deletedUrn, @Nonnull final Urn assetUrn) {
    if (deletedUrn.getEntityType().equals("form")) {
      return ImmutableList.of("forms");
    }
    if (deletedUrn.getEntityType().equals("structuredProperty")
        && assetUrn.getEntityType().equals("form")) {
      return ImmutableList.of("formInfo");
    }
    return new ArrayList<>();
  }

  /**
   * Determine whether the asset referencing the deleted urn should be deleted itself.
   *
   * <p>TODO: extend this to support other types of deletes and be more dynamic depending on aspects
   * that the asset has
   */
  private boolean shouldDeleteAssetReferencingUrn(
      @Nonnull final Urn assetUrn, @Nonnull final Urn deletedUrn) {
    if (assetUrn.getEntityType().equals("test") && deletedUrn.getEntityType().equals("form")) {
      return true;
    }
    return false;
  }

  @Nullable
  private MetadataChangeProposal updateAspectForSearchReference(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assetUrn,
      @Nonnull final Urn deletedUrn,
      @Nonnull final String aspectName) {
    if (aspectName.equals("forms")) {
      return updateFormsAspect(opContext, assetUrn, deletedUrn);
    }
    if (aspectName.equals("formInfo") && deletedUrn.getEntityType().equals("structuredProperty")) {
      return updateFormInfoAspect(opContext, assetUrn, deletedUrn);
    }
    return null;
  }

  @Nullable
  private MetadataChangeProposal updateFormsAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assetUrn,
      @Nonnull final Urn deletedUrn) {
    RecordTemplate record = _entityService.getLatestAspect(opContext, assetUrn, "forms");
    if (record == null) return null;

    return DeleteEntityUtils.removeFormFromFormsAspect(
        new Forms(record.data()), assetUrn, deletedUrn);
  }

  @Nullable
  private MetadataChangeProposal updateFormInfoAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final Urn assetUrn,
      @Nonnull final Urn deletedUrn) {
    RecordTemplate record = _entityService.getLatestAspect(opContext, assetUrn, "formInfo");
    if (record == null) return null;

    return DeleteEntityUtils.createFormInfoUpdateProposal(
        new FormInfo(record.data()), assetUrn, deletedUrn);
  }

  private AuditStamp createAuditStamp() {
    return new AuditStamp()
        .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
        .setTime(System.currentTimeMillis());
  }

  @AllArgsConstructor
  @Data
  private static class DeleteEntityServiceError {
    String message;
    DeleteEntityServiceErrorReason reason;
    Map<String, Object> context;
  }

  private enum DeleteEntityServiceErrorReason {
    ENTITY_SERVICE_ASPECT_NOT_FOUND,
    ENTITY_REGISTRY_SPEC_NOT_FOUND,
    MCP_PROCESSOR_FAILED,
    ASPECT_DELETE_FAILED,
    CLONE_FAILED,
  }

  @AllArgsConstructor
  @Data
  private static class EnrichedAspect {
    String name;
    Aspect aspect;
    AspectSpec spec;
  }
}
