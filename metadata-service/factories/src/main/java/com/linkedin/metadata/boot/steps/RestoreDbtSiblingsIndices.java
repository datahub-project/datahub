package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.DataMap;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.key.DataHubUpgradeKey;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeResult;
import io.ebean.EbeanServer;
import io.ebean.PagedList;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.*;


@Slf4j
@RequiredArgsConstructor
public class RestoreDbtSiblingsIndices implements BootstrapStep {
  private static final String VERSION = "0";
  private static final String UPGRADE_ID = "restore-dbt-siblings-indices";
  private static final Urn GLOSSARY_UPGRADE_URN =
      EntityKeyUtils.convertEntityKeyToUrn(new DataHubUpgradeKey().setId(UPGRADE_ID), Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
  private static final Integer BATCH_SIZE = 1000;
  private static final Integer SLEEP_SECONDS = 120;
  private static final long DEFAULT_BATCH_DELAY_MS = 250;

  private final EntityService _entityService;
  private final EbeanServer _ebeanServer;
  private final EntityRegistry _entityRegistry;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  @Override
  public void execute() throws Exception {
    log.info("Attempting to run RestoreDbtSiblingsIndices upgrade..");
    log.info(String.format("Waiting %s seconds..", SLEEP_SECONDS));

    // Sleep to ensure deployment process finishes.
    Thread.sleep(SLEEP_SECONDS * 1000);

    EntityResponse response = _entityService.getEntityV2(
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
        GLOSSARY_UPGRADE_URN,
        Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
    );
    if (response != null && response.getAspects().containsKey(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)) {
      DataMap dataMap = response.getAspects().get(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME).getValue().data();
      DataHubUpgradeRequest request = new DataHubUpgradeRequest(dataMap);
      if (request.hasVersion() && request.getVersion().equals(VERSION)) {
        log.info("RestoreDbtSiblingsIndices has run before with this version. Skipping");
        return;
      }
    }

    log.info("Bootstrapping sibling aspects");

    final int rowCount =
        _ebeanServer.find(EbeanAspectV2.class).where()
            .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
            .eq(EbeanAspectV2.ASPECT_COLUMN, UPSTREAM_LINEAGE_ASPECT_NAME)
            .findCount();

    log.info("Found {} upstream lineage aspects to attempt to bootstrap", rowCount);

    final AspectSpec datasetAspectSpec =
        _entityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME).getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    final AuditStamp auditStamp = new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    final DataHubUpgradeRequest upgradeRequest = new DataHubUpgradeRequest().setTimestampMs(System.currentTimeMillis()).setVersion(VERSION);
    ingestUpgradeAspect(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, upgradeRequest, auditStamp);

    int totalRowsMigrated = 0;
    int start = 0;
    int count = BATCH_SIZE;
    while (start < rowCount) {
      log.info(String.format("Reading rows %s through %s from the aspects table.", start, start + count));
      PagedList<EbeanAspectV2> rows = getPagedAspects(start, count);

      for (EbeanAspectV2 aspect : rows.getList()) {
        final RecordTemplate aspectRecord;
        // Create record from json aspect
        try {
          aspectRecord =
              EntityUtils.toAspectRecord(DATASET_ENTITY_NAME, UPSTREAM_LINEAGE_ASPECT_NAME, aspect.getMetadata(),
                  _entityRegistry);
        } catch (Exception e) {
          log.error(String.format("Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
              aspect.getMetadata(), e));
          continue;
        }

        SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(aspect.getSystemMetadata());

        // Produce MAE events for the aspect record
        try {
          _entityService.produceMetadataChangeLog(Urn.createFromString(aspect.getUrn()), DATASET_ENTITY_NAME,
              UPSTREAM_LINEAGE_ASPECT_NAME, datasetAspectSpec, null, aspectRecord, null, latestSystemMetadata,
              new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()), ChangeType.RESTATE);
        } catch (URISyntaxException e) {
          log.error(String.format("Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
              aspect.getMetadata(), e));
          continue;
        }

        totalRowsMigrated++;
      }
      log.info(String.format("Successfully sent MAEs for %s rows", totalRowsMigrated));
      start = start + count;
      try {
        TimeUnit.MILLISECONDS.sleep(DEFAULT_BATCH_DELAY_MS);
      } catch (InterruptedException e) {
        throw new RuntimeException("Thread interrupted while sleeping after successful batch migration.");
      }
    }

    final DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult().setTimestampMs(System.currentTimeMillis());
    ingestUpgradeAspect(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, upgradeResult, auditStamp);

    log.info("Successfully restored sibling aspects");
  }

  private PagedList<EbeanAspectV2> getPagedAspects(final int start, final int pageSize) {
    return _ebeanServer.find(EbeanAspectV2.class)
        .select(EbeanAspectV2.ALL_COLUMNS)
        .where()
        .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
        .eq(EbeanAspectV2.ASPECT_COLUMN, UPSTREAM_LINEAGE_ASPECT_NAME)
        .orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .orderBy()
        .asc(EbeanAspectV2.ASPECT_COLUMN)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .findPagedList();
  }

  private void ingestUpgradeAspect(String aspectName, RecordTemplate aspect, AuditStamp auditStamp) {
    final MetadataChangeProposal upgradeProposal = new MetadataChangeProposal();
    upgradeProposal.setEntityUrn(GLOSSARY_UPGRADE_URN);
    upgradeProposal.setEntityType(Constants.DATA_HUB_UPGRADE_ENTITY_NAME);
    upgradeProposal.setAspectName(aspectName);
    upgradeProposal.setAspect(GenericRecordUtils.serializeAspect(aspect));
    upgradeProposal.setChangeType(ChangeType.UPSERT);

    _entityService.ingestProposal(upgradeProposal, auditStamp);
  }
}
