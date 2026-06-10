package com.linkedin.datahub.upgrade.system.dataplatforminstances;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * System-update non-blocking step that backfills DataPlatformInstance aspects for all existing
 * entities. Skips automatically if the aspect already exists (migration already ran).
 */
@Slf4j
public class IngestDataPlatformInstancesUpgradeStep implements UpgradeStep {

  private static final String STEP_ID = "ingest-data-platform-instances";
  private static final int BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;
  private final AspectMigrationsDao _migrationsDao;
  private final boolean _enabled;

  public IngestDataPlatformInstancesUpgradeStep(
      @Nonnull final EntityService<?> entityService,
      @Nonnull final AspectMigrationsDao migrationsDao,
      final boolean enabled) {
    _entityService = entityService;
    _migrationsDao = migrationsDao;
    _enabled = enabled;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    if (!_enabled) {
      log.info("Ingest data platform instances step is disabled; skipping.");
      return true;
    }
    if (_migrationsDao.checkIfAspectExists(
        context.opContext(), DATA_PLATFORM_INSTANCE_ASPECT_NAME)) {
      log.info("DataPlatformInstance aspect exists. Skipping step.");
      return true;
    }
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        execute(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to ingest data platform instances", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
    log.info("Ingesting DataPlatformInstance aspects for all entities...");

    long numEntities = _migrationsDao.countEntities(systemOperationContext);
    long start = 0;

    while (start < numEntities) {
      log.info(
          "Reading urns {} to {} from the aspects table to generate dataplatform instance aspects",
          start,
          start + BATCH_SIZE);

      List<ChangeItemImpl> items = new LinkedList<>();
      final AuditStamp aspectAuditStamp =
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      for (String urnStr :
          _migrationsDao.listAllUrns(
              systemOperationContext, (int) start, (int) (start + BATCH_SIZE))) {
        Urn urn = Urn.createFromString(urnStr);
        Optional<DataPlatformInstance> dataPlatformInstance =
            getDataPlatformInstance(systemOperationContext, urn);
        if (dataPlatformInstance.isPresent()) {
          items.add(
              ChangeItemImpl.builder()
                  .urn(urn)
                  .aspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                  .recordTemplate(dataPlatformInstance.get())
                  .auditStamp(aspectAuditStamp)
                  .build(systemOperationContext.getAspectRetriever()));
        }
      }

      _entityService.ingestAspects(
          systemOperationContext,
          AspectsBatchImpl.builder()
              .retrieverContext(systemOperationContext.getRetrieverContext())
              .items(items)
              .build(systemOperationContext),
          true,
          true);

      log.info(
          "Finished ingesting DataPlatformInstance for urn {} to {}", start, start + BATCH_SIZE);
      start += BATCH_SIZE;
    }
    log.info("Finished ingesting DataPlatformInstance for all entities");
  }

  private Optional<DataPlatformInstance> getDataPlatformInstance(
      @Nonnull final OperationContext opContext, final Urn urn) {
    final AspectSpec keyAspectSpec = opContext.getEntityRegistryContext().getKeyAspectSpec(urn);
    RecordTemplate keyAspect = EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec);
    return DataPlatformInstanceUtils.buildDataPlatformInstance(urn.getEntityType(), keyAspect);
  }
}
