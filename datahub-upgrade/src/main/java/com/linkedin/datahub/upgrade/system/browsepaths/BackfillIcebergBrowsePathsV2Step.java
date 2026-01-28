package com.linkedin.datahub.upgrade.system.browsepaths;

import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.utils.SystemMetadataUtils.createDefaultSystemMetadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePathEntry;
import com.linkedin.common.BrowsePathEntryArray;
import com.linkedin.common.BrowsePathsV2;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.container.Container;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.key.DataPlatformInstanceKey;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BackfillIcebergBrowsePathsV2Step implements UpgradeStep {

  private static final String UPGRADE_ID = "BackfillIcebergBrowsePathsV2Step";
  private static final Urn UPGRADE_ID_URN = BootstrapStep.getUpgradeUrn(UPGRADE_ID);
  public static final String DEFAULT_BROWSE_PATH_V2 = "␟Default";

  private static final String NAMESPACE_CONTAINER_PREFIX = "urn:li:container:iceberg__";
  private static final String PLATFORM_NAME = "iceberg";

  private static final Set<String> ENTITY_TYPES_TO_MIGRATE =
      ImmutableSet.of(Constants.DATASET_ENTITY_NAME, Constants.CONTAINER_ENTITY_NAME);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final SearchService searchService;

  private final Integer batchSize;

  public BackfillIcebergBrowsePathsV2Step(
      OperationContext opContext,
      EntityService<?> entityService,
      SearchService searchService,
      Integer batchSize) {
    this.opContext = opContext;
    this.searchService = searchService;
    this.entityService = entityService;
    this.batchSize = batchSize;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      String scrollId = null;
      for (String entityType : ENTITY_TYPES_TO_MIGRATE) {
        int migratedCount = 0;
        do {
          log.info(
              String.format(
                  "Upgrading batch %s-%s of browse paths for entity type %s",
                  migratedCount, migratedCount + batchSize, entityType));
          scrollId = backfillBrowsePathsV2(entityType, auditStamp, scrollId);
          migratedCount += batchSize;
        } while (scrollId != null);
      }

      BootstrapStep.setUpgradeResult(context.opContext(), UPGRADE_ID_URN, entityService);

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  @VisibleForTesting
  String backfillBrowsePathsV2(String entityType, AuditStamp auditStamp, String scrollId) {
    final ScrollResult scrollResult =
        searchService.scrollAcrossEntities(
            opContext.withSearchFlags(
                flags ->
                    flags
                        .setFulltext(true)
                        .setSkipCache(true)
                        .setSkipHighlighting(true)
                        .setSkipAggregates(true)),
            ImmutableList.of(entityType),
            "dataplatform:iceberg",
            null,
            null,
            scrollId,
            null,
            batchSize);

    if (scrollResult.getNumEntities() == 0 || scrollResult.getEntities().size() == 0) {
      return null;
    }

    for (SearchEntity searchEntity : scrollResult.getEntities()) {
      try {
        log.info(
            "Backfilling browse paths v2 for entity type {} and entity {}",
            entityType,
            searchEntity.getEntity());
        if (Constants.CONTAINER_ENTITY_NAME.equals(entityType)) {
          ingestBrowsePathsV2ForContainer(opContext, searchEntity.getEntity(), auditStamp);
        } else if (entityType == Constants.DATASET_ENTITY_NAME) {
          ingestBrowsePathsV2ForDataset(opContext, searchEntity.getEntity(), auditStamp);
        }
      } catch (Exception e) {
        // don't stop the whole step because of one bad urn or one bad ingestion
        log.error(
            String.format(
                "Error ingesting default browsePathsV2 aspect for urn %s",
                searchEntity.getEntity()),
            e);
      }
    }

    return scrollResult.getScrollId();
  }

  Urn platformUrn() {
    return new DataPlatformUrn(PLATFORM_NAME);
  }

  Urn platformInstanceUrnFromContainerUrn(Urn urn) {
    String platformInstance =
        urn.toString().substring(NAMESPACE_CONTAINER_PREFIX.length(), urn.toString().indexOf('.'));

    DataPlatformInstanceKey platformInstanceKey =
        new DataPlatformInstanceKey().setInstance(platformInstance).setPlatform(platformUrn());
    return EntityKeyUtils.convertEntityKeyToUrn(
        platformInstanceKey, DATA_PLATFORM_INSTANCE_ENTITY_NAME);
  }

  @VisibleForTesting
  static String namespaceNameFromContainerUrn(Urn urn) {
    // Must do inverse of implementation of method containerUrn(String platformInstance, String[]
    // levels) in this file
    String namespaceWithPlatformInstance =
        urn.toString().substring(NAMESPACE_CONTAINER_PREFIX.length());
    return namespaceWithPlatformInstance.substring(namespaceWithPlatformInstance.indexOf('.') + 1);
  }

  @VisibleForTesting
  void addBrowsePathsForContainer(
      String namespaceUrnPrefix, String[] levels, BrowsePathEntryArray browsePathsArray)
      throws URISyntaxException {
    for (int level = 0; level < levels.length; level++) {
      namespaceUrnPrefix = namespaceUrnPrefix + "." + levels[level];
      browsePathsArray.add(
          new BrowsePathEntry()
              .setId(namespaceUrnPrefix)
              .setUrn(Urn.createFromString(namespaceUrnPrefix)));
    }
  }

  @VisibleForTesting
  void ingestBrowsePathsV2ForContainer(
      @Nonnull OperationContext opContext, Urn urn, AuditStamp auditStamp) throws Exception {

    // IRC created containers have URNs of the form "urn":
    // "urn:li:container:iceberg__test_wh_0.default.ns0",
    // The first part is platform instance, remaining are container/namespace heirarchy separated
    // with .

    // Ingestion created containers do have correct names.

    RecordTemplate browsePathsV2Data =
        entityService.getLatestAspect(opContext, urn, BROWSE_PATHS_V2_ASPECT_NAME);

    if (!urn.toString().startsWith(NAMESPACE_CONTAINER_PREFIX)) {
      return; // This is not an IRC created container. nothing to do
    }

    String namespace = namespaceNameFromContainerUrn(urn);
    String[] levels = namespace.split("\\.");

    Urn platformInstanceUrn = platformInstanceUrnFromContainerUrn(urn);

    BrowsePathsV2 browsePathsV2 = new BrowsePathsV2();
    BrowsePathEntryArray browsePathsArray = new BrowsePathEntryArray();

    browsePathsArray.add(
        new BrowsePathEntry().setId(platformInstanceUrn.toString()).setUrn(platformInstanceUrn));

    String namespaceUrnPrefix = urn.toString().substring(0, urn.toString().indexOf('.'));
    if (levels.length
        > 1) { // We add browse paths upto parent container. So, for root namespace, we stop at
      // platform instance.
      addBrowsePathsForContainer(
          namespaceUrnPrefix, Arrays.copyOfRange(levels, 0, levels.length - 1), browsePathsArray);
    }
    browsePathsV2.setPath(browsePathsArray);

    log.info(String.format("Adding browse path v2 for urn %s with value %s", urn, browsePathsV2));

    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(GenericRecordUtils.serializeAspect(browsePathsV2));
    entityService.ingestProposal(opContext, proposal, auditStamp, true);
  }

  @VisibleForTesting
  void ingestBrowsePathsV2ForDataset(
      @Nonnull OperationContext opContext, Urn urn, AuditStamp auditStamp) throws Exception {

    RecordTemplate browsePathsV2Data =
        entityService.getLatestAspect(opContext, urn, BROWSE_PATHS_V2_ASPECT_NAME);
    RecordTemplate containerData =
        entityService.getLatestAspect(opContext, urn, CONTAINER_ASPECT_NAME);
    Container container = new Container(containerData.data());

    Urn containerUrn = container.getContainer();

    if (!containerUrn.toString().startsWith(NAMESPACE_CONTAINER_PREFIX)) {
      return; // This is not an IRC created container. nothing to do
    }

    String namespace = namespaceNameFromContainerUrn(containerUrn);
    String[] levels = namespace.split("\\.");

    Urn platformInstanceUrn = platformInstanceUrnFromContainerUrn(containerUrn);

    BrowsePathsV2 browsePathsV2 = new BrowsePathsV2();
    BrowsePathEntryArray browsePathsArray = new BrowsePathEntryArray();

    browsePathsArray.add(
        new BrowsePathEntry().setId(platformInstanceUrn.toString()).setUrn(platformInstanceUrn));

    String namespaceUrnPrefix =
        containerUrn.toString().substring(0, containerUrn.toString().indexOf('.'));
    if (levels.length > 0) { // For dataset, all levels of container are part of browse path
      addBrowsePathsForContainer(
          namespaceUrnPrefix, Arrays.copyOfRange(levels, 0, levels.length), browsePathsArray);
    }
    browsePathsV2.setPath(browsePathsArray);

    log.info(String.format("Adding browse path v2 for urn %s with value %s", urn, browsePathsV2));

    MetadataChangeProposal proposal = new MetadataChangeProposal();
    proposal.setEntityUrn(urn);
    proposal.setEntityType(urn.getEntityType());
    proposal.setAspectName(Constants.BROWSE_PATHS_V2_ASPECT_NAME);
    proposal.setChangeType(ChangeType.UPSERT);
    proposal.setSystemMetadata(createDefaultSystemMetadata());
    proposal.setAspect(GenericRecordUtils.serializeAspect(browsePathsV2));

    entityService.ingestProposal(opContext, proposal, auditStamp, true);
  }

  @Override
  public String id() {
    return UPGRADE_ID;
  }

  /**
   * Returns whether the upgrade should proceed if the step fails after exceeding the maximum
   * retries.
   */
  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  /** Returns whether the upgrade should be skipped. Uses previous run history */
  public boolean skip(UpgradeContext context) {
    boolean previouslyRun =
        entityService.exists(
            context.opContext(), UPGRADE_ID_URN, DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, true);
    if (previouslyRun) {
      log.info("{} was already run. Skipping.", id());
    }
    return previouslyRun;
  }
}
