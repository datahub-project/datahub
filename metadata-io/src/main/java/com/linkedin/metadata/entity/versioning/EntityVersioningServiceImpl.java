package com.linkedin.metadata.entity.versioning;

import static com.linkedin.metadata.Constants.INITIAL_VERSION_SORT_ID;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.search.utils.QueryUtils.EMPTY_FILTER;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.entity.Aspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.versionset.VersionSetProperties;
import com.linkedin.versionset.VersioningScheme;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityVersioningServiceImpl implements EntityVersioningService {

  private final EntityService<?> entityService;

  public EntityVersioningServiceImpl(EntityService<?> entityService) {
    this.entityService = entityService;
  }

  /**
   * Generates a new set of VersionProperties for the latest version and links it to the specified
   * version set. If the specified version set does not yet exist, will create it. Order of
   * operations here is important: 1. Create initial Version Set if necessary, do not generate
   * Version Set Properties 2. Create Version Properties for specified entity. If this aspect
   * already exists will fail. 3. Generate version properties with the properly set latest version
   * Will eventually want to add in the scheme here as a parameter
   *
   * @return ingestResult -> the results of the ingested linked version
   */
  @Override
  public List<IngestResult> linkLatestVersion(
      OperationContext opContext,
      Urn versionSet,
      Urn newLatestVersion,
      VersionPropertiesInput inputProperties) {
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    String sortId;
    if (!aspectRetriever.entityExists(ImmutableSet.of(versionSet)).get(versionSet)) {
      VersionSetKey versionSetKey =
          (VersionSetKey)
              EntityKeyUtils.convertUrnToEntityKey(
                  versionSet, opContext.getEntityRegistryContext().getKeyAspectSpec(versionSet));
      if (!versionSetKey.getEntityType().equals(newLatestVersion.getEntityType())) {
        throw new IllegalArgumentException(
            "Entity type must match Version Set's specified type: "
                + versionSetKey.getEntityType()
                + " invalid type: "
                + newLatestVersion.getEntityType());
      }
      MetadataChangeProposal versionSetKeyProposal = new MetadataChangeProposal();
      versionSetKeyProposal.setEntityUrn(versionSet);
      versionSetKeyProposal.setEntityType(VERSION_SET_ENTITY_NAME);
      versionSetKeyProposal.setAspectName(VERSION_SET_KEY_ASPECT_NAME);
      versionSetKeyProposal.setAspect(GenericRecordUtils.serializeAspect(versionSetKey));
      versionSetKeyProposal.setChangeType(ChangeType.CREATE_ENTITY);
      proposals.add(versionSetKeyProposal);
      sortId = INITIAL_VERSION_SORT_ID;
    } else {
      Aspect versionSetPropertiesAspect =
          aspectRetriever.getLatestAspectObject(versionSet, VERSION_PROPERTIES_ASPECT_NAME);
      VersionSetProperties versionSetProperties =
          RecordUtils.toRecordTemplate(
              VersionSetProperties.class, versionSetPropertiesAspect.data());
      Aspect latestVersion =
          aspectRetriever.getLatestAspectObject(
              versionSetProperties.getLatest(), VERSION_PROPERTIES_ASPECT_NAME);
      VersionProperties latestVersionProperties =
          RecordUtils.toRecordTemplate(VersionProperties.class, latestVersion.data());
      // When more impls for versioning scheme are set up, this will need to be resolved to the
      // correct scheme generation strategy
      sortId = AlphanumericSortIdGenerator.increment(latestVersionProperties.getSortId());
    }

    VersionProperties versionProperties =
        new VersionProperties()
            .setVersionSet(versionSet)
            .setComment(inputProperties.getComment(), SetMode.IGNORE_NULL)
            .setLabel(inputProperties.getLabel())
            .setMetadataCreatedTimestamp(opContext.getAuditStamp())
            .setSortId(sortId);
    if (inputProperties.getSourceCreationTimestamp() != null) {

      AuditStamp sourceCreatedAuditStamp =
          new AuditStamp().setTime(inputProperties.getSourceCreationTimestamp());
      Urn actor = null;
      if (inputProperties.getSourceCreator() != null) {
        actor = new CorpuserUrn(inputProperties.getSourceCreator());
      }
      sourceCreatedAuditStamp.setActor(UrnUtils.getActorOrDefault(actor));

      versionProperties.setSourceCreatedTimestamp(sourceCreatedAuditStamp);
    }
    MetadataChangeProposal versionPropertiesProposal = new MetadataChangeProposal();
    versionPropertiesProposal.setEntityUrn(newLatestVersion);
    versionPropertiesProposal.setEntityType(newLatestVersion.getEntityType());
    versionPropertiesProposal.setAspectName(VERSION_PROPERTIES_ASPECT_NAME);
    versionPropertiesProposal.setAspect(GenericRecordUtils.serializeAspect(versionProperties));
    // Error if properties already exist
    versionPropertiesProposal.setChangeType(ChangeType.CREATE);
    proposals.add(versionPropertiesProposal);

    // Might want to refactor this to a Patch w/ Create if not exists logic if more properties get
    // added
    // to Version Set Properties
    VersionSetProperties versionSetProperties =
        new VersionSetProperties()
            .setVersioningScheme(
                VersioningScheme
                    .ALPHANUMERIC_GENERATED_BY_DATAHUB) // Only one available, will need to add to
            // input properties once more are added.
            .setLatest(newLatestVersion);
    MetadataChangeProposal versionSetPropertiesProposal = new MetadataChangeProposal();
    versionSetPropertiesProposal.setEntityUrn(versionSet);
    versionSetPropertiesProposal.setEntityType(VERSION_SET_ENTITY_NAME);
    versionSetPropertiesProposal.setAspectName(VERSION_SET_PROPERTIES_ASPECT_NAME);
    versionSetPropertiesProposal.setAspect(
        GenericRecordUtils.serializeAspect(versionSetProperties));
    versionSetPropertiesProposal.setChangeType(ChangeType.UPSERT);
    proposals.add(versionSetPropertiesProposal);

    return entityService.ingestProposal(
        opContext,
        AspectsBatchImpl.builder()
            .mcps(proposals, opContext.getAuditStamp(), opContext.getRetrieverContext().get())
            .build(),
        false);
  }

  /**
   * Unlinks the latest version from a version set. Will attempt to set up the previous version as
   * the new latest. This fully removes the version properties and unversions the specified entity.
   *
   * @param opContext operational context containing various information about the current execution
   * @param currentLatest the currently linked latest versioned entity urn
   * @return the deletion result
   */
  @Override
  public List<RollbackResult> unlinkLatestVersion(OperationContext opContext, Urn currentLatest) {
    List<RollbackResult> deletedAspects = new ArrayList<>();
    AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    Aspect latestVersionPropertiesAspect =
        aspectRetriever.getLatestAspectObject(currentLatest, VERSION_PROPERTIES_ASPECT_NAME);
    // Not currently versioned, do nothing
    if (latestVersionPropertiesAspect == null) {
      return deletedAspects;
    }
    VersionProperties latestVersionProperties =
        RecordUtils.toRecordTemplate(VersionProperties.class, latestVersionPropertiesAspect.data());
    Urn versionSetUrn = latestVersionProperties.getVersionSet();
    // If initial version, delete the version set properties as well (technically if there was a
    // loopback would
    // potentially be problematic, but this requires a couple of hundred billion versions and is
    // unlikely)
    if (INITIAL_VERSION_SORT_ID.equals(latestVersionProperties.getSortId())) {
      entityService
          .deleteAspect(
              opContext,
              versionSetUrn.toString(),
              VERSION_SET_PROPERTIES_ASPECT_NAME,
              Collections.emptyMap(),
              true)
          .ifPresent(deletedAspects::add);
    } else {
      // Determine previous version
      VersionSetKey versionSetKey =
          (VersionSetKey)
              EntityKeyUtils.convertUrnToEntityKey(
                  versionSetUrn,
                  opContext.getEntityRegistryContext().getKeyAspectSpec(versionSetUrn));
      GraphRetriever graphRetriever = opContext.getRetrieverContext().get().getGraphRetriever();
      RelatedEntitiesScrollResult versionRelationships =
          graphRetriever.scrollRelatedEntities(
              ImmutableList.of(versionSetKey.getEntityType()),
              QueryUtils.newConjunctiveFilter(
                  CriterionUtils.buildCriterion(
                      "versionSet", Condition.EQUAL, versionSetUrn.toString())),
              null,
              EMPTY_FILTER,
              ImmutableList.of("VersionOf"),
              QueryUtils.newRelationshipFilter(EMPTY_FILTER, RelationshipDirection.INCOMING),
              ImmutableList.of(
                  new SortCriterion().setField("sortId").setOrder(SortOrder.DESCENDING)),
              null,
              // Grab current latest and one before it
              2,
              null,
              null);
      String updatedLatestVersionUrn = null;
      List<RelatedEntities> linkedEntities = versionRelationships.getEntities();
      if (linkedEntities.size() == 2) {
        // Happy path, all expected data is there. Get second to latest and set to the prior latest
        // version.
        // Note: this relies on Alpha sorting for the sort id, for different versioning schemes may
        // need to enforce
        // strict order at the search index
        RelatedEntities priorLatestVersion = linkedEntities.get(1);
        updatedLatestVersionUrn = priorLatestVersion.getDestinationUrn();
      } else if (versionRelationships.getEntities().size() == 1) {
        // Missing a version, if that version is not the one being unlinked then set as latest
        // version
        RelatedEntities maybePriorLatestVersion = linkedEntities.get(0);
        if (!currentLatest.equals(UrnUtils.getUrn(maybePriorLatestVersion.getDestinationUrn()))) {
          updatedLatestVersionUrn = maybePriorLatestVersion.getDestinationUrn();
        }
        // Otherwise version has been updated, but previous versions have all been deleted.
        // Delete Version Set like if only initial version remains
      }

      if (updatedLatestVersionUrn == null) {
        // One of:
        // 1. Missing all data
        // 2. Somehow got more back than requested
        // 3. Version has been updated, but previous versions have all been deleted.
        // Delete Version Set like if only initial version remains
        entityService
            .deleteAspect(
                opContext,
                versionSetUrn.toString(),
                VERSION_SET_PROPERTIES_ASPECT_NAME,
                Collections.emptyMap(),
                true)
            .ifPresent(deletedAspects::add);
      } else {

        // Might want to refactor this to a Patch w/ Create if not exists logic if more properties
        // get added
        // to Version Set Properties
        VersionSetProperties versionSetProperties =
            new VersionSetProperties()
                .setVersioningScheme(
                    VersioningScheme
                        .ALPHANUMERIC_GENERATED_BY_DATAHUB) // Only one available, will need to add
                // to input properties once more are
                // added.
                .setLatest(UrnUtils.getUrn(updatedLatestVersionUrn));
        MetadataChangeProposal versionSetPropertiesProposal = new MetadataChangeProposal();
        versionSetPropertiesProposal.setEntityUrn(versionSetUrn);
        versionSetPropertiesProposal.setEntityType(VERSION_SET_ENTITY_NAME);
        versionSetPropertiesProposal.setAspectName(VERSION_SET_PROPERTIES_ASPECT_NAME);
        versionSetPropertiesProposal.setAspect(
            GenericRecordUtils.serializeAspect(versionSetProperties));
        versionSetPropertiesProposal.setChangeType(ChangeType.UPSERT);
        entityService.ingestProposal(
            opContext,
            AspectsBatchImpl.builder()
                .mcps(
                    ImmutableList.of(versionSetPropertiesProposal),
                    opContext.getAuditStamp(),
                    opContext.getRetrieverContext().get())
                .build(),
            false);
      }
    }

    // Delete latest version properties
    entityService
        .deleteAspect(
            opContext,
            currentLatest.toString(),
            VERSION_PROPERTIES_ASPECT_NAME,
            Collections.emptyMap(),
            true)
        .ifPresent(deletedAspects::add);

    return deletedAspects;
  }
}
