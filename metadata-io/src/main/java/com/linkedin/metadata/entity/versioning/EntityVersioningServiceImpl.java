package com.linkedin.metadata.entity.versioning;

import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.INITIAL_VERSION_SORT_ID;
import static com.linkedin.metadata.Constants.UI_SOURCE;
import static com.linkedin.metadata.Constants.VERSION_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_KEY_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SET_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.VERSION_SORT_ID_FIELD_NAME;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.datahub.util.RecordUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.MetadataAttribution;
import com.linkedin.common.VersionProperties;
import com.linkedin.common.VersionTag;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.IngestResult;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.key.VersionSetKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.utils.QueryUtils;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
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
    List<IngestResult> ingestResults = new ArrayList<>();
    AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    String sortId;
    Long versionSetConstraint;
    Long versionPropertiesConstraint = -1L;
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
    if (!aspectRetriever.entityExists(ImmutableSet.of(versionSet)).get(versionSet)) {
      MetadataChangeProposal versionSetKeyProposal = new MetadataChangeProposal();
      versionSetKeyProposal.setEntityUrn(versionSet);
      versionSetKeyProposal.setEntityType(VERSION_SET_ENTITY_NAME);
      versionSetKeyProposal.setAspectName(VERSION_SET_KEY_ASPECT_NAME);
      versionSetKeyProposal.setAspect(GenericRecordUtils.serializeAspect(versionSetKey));
      versionSetKeyProposal.setChangeType(ChangeType.CREATE_ENTITY);
      ingestResults.add(
          entityService.ingestProposal(
              opContext, versionSetKeyProposal, opContext.getAuditStamp(), false));

      sortId = INITIAL_VERSION_SORT_ID;
      versionSetConstraint = -1L;
    } else {
      SystemAspect versionSetPropertiesAspect =
          aspectRetriever.getLatestSystemAspect(versionSet, VERSION_SET_PROPERTIES_ASPECT_NAME);
      VersionSetProperties versionSetProperties =
          RecordUtils.toRecordTemplate(
              VersionSetProperties.class, versionSetPropertiesAspect.getRecordTemplate().data());
      versionSetConstraint =
          versionSetPropertiesAspect
              .getSystemMetadataVersion()
              .orElse(versionSetPropertiesAspect.getVersion());
      SystemAspect latestVersion =
          aspectRetriever.getLatestSystemAspect(
              versionSetProperties.getLatest(), VERSION_PROPERTIES_ASPECT_NAME);
      VersionProperties latestVersionProperties =
          RecordUtils.toRecordTemplate(
              VersionProperties.class, latestVersion.getRecordTemplate().data());
      // When more impls for versioning scheme are set up, this will need to be resolved to the
      // correct scheme generation strategy
      sortId = AlphanumericSortIdGenerator.increment(latestVersionProperties.getSortId());
    }

    SystemAspect currentVersionPropertiesAspect =
        aspectRetriever.getLatestSystemAspect(newLatestVersion, VERSION_PROPERTIES_ASPECT_NAME);
    if (currentVersionPropertiesAspect != null) {
      VersionProperties currentVersionProperties =
          RecordUtils.toRecordTemplate(
              VersionProperties.class, currentVersionPropertiesAspect.getRecordTemplate().data());
      if (currentVersionProperties.getVersionSet().equals(versionSet)) {
        return new ArrayList<>();
      } else {
        throw new IllegalStateException(
            String.format(
                "Version already exists for specified entity: %s for a different Version Set: %s",
                newLatestVersion, currentVersionProperties.getVersionSet()));
      }
    }

    VersionTag versionTag = new VersionTag();
    versionTag.setVersionTag(inputProperties.getVersion());
    MetadataAttribution metadataAttribution = new MetadataAttribution();
    metadataAttribution.setActor(opContext.getActorContext().getActorUrn());
    metadataAttribution.setTime(System.currentTimeMillis());
    versionTag.setMetadataAttribution(metadataAttribution);
    VersionProperties versionProperties =
        new VersionProperties()
            .setVersionSet(versionSet)
            .setComment(inputProperties.getComment(), SetMode.IGNORE_NULL)
            .setVersion(versionTag)
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
    SystemMetadata systemMetadata = createDefaultSystemMetadata();
    StringMap properties = new StringMap();
    properties.put(APP_SOURCE, UI_SOURCE);
    systemMetadata.setProperties(properties);
    versionPropertiesProposal.setSystemMetadata(systemMetadata);
    versionPropertiesProposal.setEntityUrn(newLatestVersion);
    versionPropertiesProposal.setEntityType(newLatestVersion.getEntityType());
    versionPropertiesProposal.setAspectName(VERSION_PROPERTIES_ASPECT_NAME);
    versionPropertiesProposal.setAspect(GenericRecordUtils.serializeAspect(versionProperties));
    versionPropertiesProposal.setChangeType(ChangeType.UPSERT);
    StringMap headerMap = new StringMap();
    headerMap.put(HTTP_HEADER_IF_VERSION_MATCH, versionPropertiesConstraint.toString());
    versionPropertiesProposal.setChangeType(ChangeType.UPSERT);
    versionPropertiesProposal.setHeaders(headerMap);
    ingestResults.add(
        entityService.ingestProposal(
            opContext, versionPropertiesProposal, opContext.getAuditStamp(), false));

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
    StringMap versionSetHeaderMap = new StringMap();
    versionSetHeaderMap.put(HTTP_HEADER_IF_VERSION_MATCH, versionSetConstraint.toString());
    versionSetPropertiesProposal.setHeaders(versionSetHeaderMap);
    versionSetPropertiesProposal.setSystemMetadata(systemMetadata);
    ingestResults.add(
        entityService.ingestProposal(
            opContext, versionSetPropertiesProposal, opContext.getAuditStamp(), false));

    return ingestResults;
  }

  /**
   * Unlinks a version from a version set. Will attempt to set up the previous version as the new
   * latest. This fully removes the version properties and unversions the specified entity.
   *
   * @param opContext operational context containing various information about the current execution
   * @param linkedVersion the currently linked latest versioned entity urn
   * @return the deletion result
   */
  @Override
  public List<RollbackResult> unlinkVersion(
      OperationContext opContext, Urn versionSet, Urn linkedVersion) {
    List<RollbackResult> deletedAspects = new ArrayList<>();
    AspectRetriever aspectRetriever = opContext.getAspectRetriever();
    SystemAspect linkedVersionPropertiesAspect =
        aspectRetriever.getLatestSystemAspect(linkedVersion, VERSION_PROPERTIES_ASPECT_NAME);
    // Not currently versioned, do nothing
    if (linkedVersionPropertiesAspect == null) {
      return deletedAspects;
    }
    VersionProperties linkedVersionProperties =
        RecordUtils.toRecordTemplate(
            VersionProperties.class, linkedVersionPropertiesAspect.getRecordTemplate().data());
    Urn versionSetUrn = linkedVersionProperties.getVersionSet();
    if (!versionSet.equals(versionSetUrn)) {
      throw new IllegalArgumentException(
          String.format(
              "Version is not linked to specified version set: %s but is linked to: %s",
              versionSet, versionSetUrn));
    }
    // Delete linked version properties
    entityService
        .deleteAspect(
            opContext,
            linkedVersion.toString(),
            VERSION_PROPERTIES_ASPECT_NAME,
            Collections.emptyMap(),
            true,
            true)
        .ifPresent(deletedAspects::add);

    // Get Version Set details
    VersionSetKey versionSetKey =
        (VersionSetKey)
            EntityKeyUtils.convertUrnToEntityKey(
                versionSetUrn,
                opContext.getEntityRegistryContext().getKeyAspectSpec(versionSetUrn));

    SystemAspect versionSetPropertiesAspect =
        aspectRetriever.getLatestSystemAspect(versionSetUrn, VERSION_SET_PROPERTIES_ASPECT_NAME);
    if (versionSetPropertiesAspect == null) {
      throw new IllegalStateException(
          String.format(
              "Version Set Properties must exist if entity version exists: %s", versionSetUrn));
    }
    VersionSetProperties versionSetProperties =
        RecordUtils.toRecordTemplate(
            VersionSetProperties.class, versionSetPropertiesAspect.getRecordTemplate().data());
    long versionConstraint =
        versionSetPropertiesAspect
            .getSystemMetadataVersion()
            .orElse(versionSetPropertiesAspect.getVersion());
    boolean isLatest = linkedVersion.equals(versionSetProperties.getLatest());
    SearchRetriever searchRetriever = opContext.getRetrieverContext().getSearchRetriever();

    String updatedLatestVersionUrn = null;
    if (isLatest) {

      // Find current latest version and previous
      ScrollResult linkedVersions =
          searchRetriever.scroll(
              ImmutableList.of(versionSetKey.getEntityType()),
              QueryUtils.newConjunctiveFilter(
                  CriterionUtils.buildCriterion(
                      "versionSet", Condition.EQUAL, versionSetUrn.toString()),
                  CriterionUtils.buildCriterion(
                      VERSION_SORT_ID_FIELD_NAME,
                      Condition.LESS_THAN,
                      linkedVersionProperties.getSortId())),
              null,
              2,
              ImmutableList.of(
                  new SortCriterion()
                      .setField(VERSION_SORT_ID_FIELD_NAME)
                      .setOrder(SortOrder.DESCENDING)),
              SearchRetriever.RETRIEVER_SEARCH_FLAGS_NO_CACHE_ALL_VERSIONS);

      SearchEntityArray linkedEntities = linkedVersions.getEntities();

      if (!linkedEntities.isEmpty()) {
        // There is a version left, set to latest linked version that is less than the current sort
        // id
        updatedLatestVersionUrn = linkedEntities.get(0).getEntity().toString();
      } else {
        // No versions left, remove the version set
        log.info("No more versions in version set: {}, deleting.", versionSetUrn);
        entityService
            .deleteAspect(
                opContext,
                versionSetUrn.toString(),
                VERSION_SET_KEY_ASPECT_NAME,
                Collections.emptyMap(),
                true,
                true)
            .ifPresent(deletedAspects::add);
      }
    }

    if (updatedLatestVersionUrn != null) {

      // Might want to refactor this to a Patch w/ Create if not exists logic if more properties
      // get added
      // to Version Set Properties
      log.info("Setting new latest version: {}", updatedLatestVersionUrn);
      VersionSetProperties newVersionSetProperties =
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
          GenericRecordUtils.serializeAspect(newVersionSetProperties));
      versionSetPropertiesProposal.setChangeType(ChangeType.UPSERT);
      StringMap headerMap = new StringMap();
      headerMap.put(HTTP_HEADER_IF_VERSION_MATCH, Long.toString(versionConstraint));
      versionSetPropertiesProposal.setHeaders(headerMap);
      SystemMetadata systemMetadata = createDefaultSystemMetadata();
      StringMap properties = new StringMap();
      properties.put(APP_SOURCE, UI_SOURCE);
      systemMetadata.setProperties(properties);
      versionSetPropertiesProposal.setSystemMetadata(systemMetadata);
      entityService.ingestProposal(
          opContext,
          AspectsBatchImpl.builder()
              .mcps(
                  ImmutableList.of(versionSetPropertiesProposal),
                  opContext.getAuditStamp(),
                  opContext.getRetrieverContext())
              .build(),
          false);
    }

    return deletedAspects;
  }
}
