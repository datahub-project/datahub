package com.linkedin.datahub.upgrade.restoreindices;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.nocode.NoCodeUpgrade;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.mxe.SystemMetadata;
import io.ebean.EbeanServer;
import io.ebean.PagedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.linkedin.metadata.Constants.ASPECT_LATEST_VERSION;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;


public class SendMAEStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long DEFAULT_BATCH_DELAY_MS = 250;

  private final EbeanServer _server;
  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;

  public SendMAEStep(final EbeanServer server, final EntityService entityService, final EntityRegistry entityRegistry) {
    _server = server;
    _entityService = entityService;
    _entityRegistry = entityRegistry;
  }

  @Override
  public String id() {
    return "SendMAEStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Sending MAE from local DB...");
      final int rowCount =
          _server.find(EbeanAspectV2.class).where().eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION).findCount();
      context.report().addLine(String.format("Found %s latest aspects in aspects table", rowCount));

      int totalRowsMigrated = 0;
      int start = 0;
      int count = getBatchSize(context.parsedArgs());
      while (start < rowCount) {

        context.report()
            .addLine(String.format("Reading rows %s through %s from the aspects table.", start, start + count));
        PagedList<EbeanAspectV2> rows = getPagedAspects(start, count);

        for (EbeanAspectV2 aspect : rows.getList()) {
          // 1. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(aspect.getKey().getUrn());
          } catch (Exception e) {
            context.report()
                .addLine(String.format("Failed to bind Urn with value %s into Urn object: %s. Ignoring row.",
                    aspect.getKey().getUrn(), e));
            continue;
          }

          // 2. Verify that the entity associated with the aspect is found in the registry.
          final String entityName = urn.getEntityType();
          final EntitySpec entitySpec;
          try {
            entitySpec = _entityRegistry.getEntitySpec(entityName);
          } catch (Exception e) {
            context.report()
                .addLine(String.format("Failed to find entity with name %s in Entity Registry: %s. Ignoring row.",
                    entityName, e));
            continue;
          }
          final String aspectName = aspect.getKey().getAspect();

          // 3. Verify that the aspect is a valid aspect associated with the entity
          AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
          if (aspectSpec == null) {
            context.report()
                .addLine(String.format("Failed to find aspect with name %s associated with entity named %s", aspectName,
                    entityName));
            continue;
          }

          // 4. Create record from json aspect
          final RecordTemplate aspectRecord;
          try {
            aspectRecord = EntityUtils.toAspectRecord(entityName, aspectName, aspect.getMetadata(), _entityRegistry);
          } catch (Exception e) {
            context.report()
                .addLine(String.format("Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
                    aspect.getMetadata(), entityName, aspectName, e));
            continue;
          }

          SystemMetadata latestSystemMetadata = EntityUtils.parseSystemMetadata(aspect.getSystemMetadata());

          // 5. Produce MAE events for the aspect record
          _entityService.produceMetadataChangeLog(urn, entityName, aspectName, aspectSpec, null, aspectRecord, null,
              latestSystemMetadata,
              new AuditStamp().setActor(UrnUtils.getUrn(SYSTEM_ACTOR)).setTime(System.currentTimeMillis()),
              ChangeType.RESTATE);

          totalRowsMigrated++;
        }
        context.report().addLine(String.format("Successfully sent MAEs for %s rows", totalRowsMigrated));
        start = start + count;
        try {
          TimeUnit.MILLISECONDS.sleep(getBatchDelayMs(context.parsedArgs()));
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread interrupted while sleeping after successful batch migration.");
        }
      }
      if (totalRowsMigrated != rowCount) {
        context.report().addLine(String.format("Failed to send MAEs for %d rows...", rowCount - totalRowsMigrated));
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private PagedList<EbeanAspectV2> getPagedAspects(final int start, final int pageSize) {
    return _server.find(EbeanAspectV2.class)
        .select(EbeanAspectV2.ALL_COLUMNS)
        .where()
        .eq(EbeanAspectV2.VERSION_COLUMN, ASPECT_LATEST_VERSION)
        .orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .orderBy()
        .asc(EbeanAspectV2.ASPECT_COLUMN)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .findPagedList();
  }

  private int getBatchSize(final Map<String, Optional<String>> parsedArgs) {
    int resolvedBatchSize = DEFAULT_BATCH_SIZE;
    if (parsedArgs.containsKey(RestoreIndices.BATCH_SIZE_ARG_NAME) && parsedArgs.get(NoCodeUpgrade.BATCH_SIZE_ARG_NAME)
        .isPresent()) {
      resolvedBatchSize = Integer.parseInt(parsedArgs.get(RestoreIndices.BATCH_SIZE_ARG_NAME).get());
    }
    return resolvedBatchSize;
  }

  private long getBatchDelayMs(final Map<String, Optional<String>> parsedArgs) {
    long resolvedBatchDelayMs = DEFAULT_BATCH_DELAY_MS;
    if (parsedArgs.containsKey(RestoreIndices.BATCH_DELAY_MS_ARG_NAME) && parsedArgs.get(
        NoCodeUpgrade.BATCH_DELAY_MS_ARG_NAME).isPresent()) {
      resolvedBatchDelayMs = Long.parseLong(parsedArgs.get(RestoreIndices.BATCH_DELAY_MS_ARG_NAME).get());
    }
    return resolvedBatchDelayMs;
  }
}
