package com.linkedin.metadata.boot.steps;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.DataPlatformInstance;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectMigrationsDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.transactions.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.transactions.UpsertBatchItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.utils.DataPlatformInstanceUtils;
import com.linkedin.metadata.utils.EntityKeyUtils;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class IngestDataPlatformInstancesStep implements BootstrapStep {

  private static final int BATCH_SIZE = 1000;

  private final EntityService _entityService;
  private final AspectMigrationsDao _migrationsDao;

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private Optional<DataPlatformInstance> getDataPlatformInstance(Urn urn) {
    final AspectSpec keyAspectSpec = _entityService.getKeyAspectSpec(urn);
    RecordTemplate keyAspect = EntityKeyUtils.convertUrnToEntityKey(urn, keyAspectSpec);
    return DataPlatformInstanceUtils.buildDataPlatformInstance(urn.getEntityType(), keyAspect);
  }

  @Override
  public void execute() throws Exception {
    log.info("Checking for DataPlatformInstance");
    if (_migrationsDao.checkIfAspectExists(DATA_PLATFORM_INSTANCE_ASPECT_NAME)) {
      log.info("DataPlatformInstance aspect exists. Skipping step");
      return;
    }

    long numEntities = _migrationsDao.countEntities();
    int start = 0;

    while (start < numEntities) {
      log.info(
          "Reading urns {} to {} from the aspects table to generate dataplatform instance aspects",
          start,
          start + BATCH_SIZE);

      List<UpsertBatchItem> items = new LinkedList<>();

      for (String urnStr : _migrationsDao.listAllUrns(start, start + BATCH_SIZE)) {
        Urn urn = Urn.createFromString(urnStr);
        Optional<DataPlatformInstance> dataPlatformInstance = getDataPlatformInstance(urn);
        if (dataPlatformInstance.isPresent()) {
          items.add(
              UpsertBatchItem.builder()
                  .urn(urn)
                  .aspectName(DATA_PLATFORM_INSTANCE_ASPECT_NAME)
                  .aspect(dataPlatformInstance.get())
                  .build(_entityService.getEntityRegistry()));
        }
      }

      final AuditStamp aspectAuditStamp =
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());
      _entityService.ingestAspects(
          AspectsBatchImpl.builder().items(items).build(), aspectAuditStamp, true, true);

      log.info(
          "Finished ingesting DataPlatformInstance for urn {} to {}", start, start + BATCH_SIZE);
      start += BATCH_SIZE;
    }
    log.info("Finished ingesting DataPlatformInstance for all entities");
  }
}
