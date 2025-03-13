package com.linkedin.datahub.upgrade.restoreindices;

import static com.linkedin.datahub.upgrade.restoreindices.RestoreIndices.CREATE_DEFAULT_ASPECTS_ARG_NAME;
import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableBiMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.BackupReader;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.BackupReaderArgs;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.EbeanAspectBackupIterator;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.LocalParquetReader;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.ParquetReaderWrapper;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.S3BackupReader;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreFromParquetStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final int DEFAULT_THREAD_POOL = 4;

  private final OperationContext systemOperationContext;
  private final EntityService<?> _entityService;
  private final Map<String, Class<? extends BackupReader<ParquetReaderWrapper>>> _backupReaders;
  private final ExecutorService _fileReaderThreadPool;
  private AtomicInteger _numRows = new AtomicInteger(0);
  private ConcurrentHashMap<String, AtomicInteger> _entityCounts = new ConcurrentHashMap<>();

  public RestoreFromParquetStep(
      @Nonnull OperationContext systemOperationContext, final EntityService<?> entityService) {
    _entityService = entityService;
    this.systemOperationContext = systemOperationContext;
    _backupReaders =
        ImmutableBiMap.of(
            LocalParquetReader.READER_NAME,
            LocalParquetReader.class,
            S3BackupReader.READER_NAME,
            S3BackupReader.class);
    String poolSize = System.getenv(RestoreIndices.READER_POOL_SIZE);
    int intPoolSize;
    try {
      intPoolSize = Integer.parseInt(poolSize);
    } catch (NumberFormatException e) {
      intPoolSize = DEFAULT_THREAD_POOL;
    }
    _fileReaderThreadPool = Executors.newFixedThreadPool(intPoolSize);
  }

  @Override
  public String id() {
    return "RestoreFromParquetStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    if (Boolean.parseBoolean(System.getenv(RestoreIndices.RESTORE_FROM_PARQUET))) {
      return false;
    }

    return true;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      context.report().addLine("Restoring indices from parquet file...");
      int numRows = 0;
      long initialStartTime = System.currentTimeMillis();
      String backupReaderName = System.getenv("BACKUP_READER");
      if (backupReaderName == null || !_backupReaders.containsKey(backupReaderName)) {
        context.report().addLine("BACKUP_READER is not set or is not valid: " + backupReaderName);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      Class<? extends BackupReader<ParquetReaderWrapper>> clazz =
          _backupReaders.get(backupReaderName);
      List<String> argNames = BackupReaderArgs.getArgNames(clazz);
      List<String> args =
          argNames.stream()
              .map(System::getenv)
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
      BackupReader<ParquetReaderWrapper> backupReader;
      try {
        backupReader = clazz.getConstructor(List.class).newInstance(args);
      } catch (InstantiationException
          | InvocationTargetException
          | IllegalAccessException
          | NoSuchMethodException e) {
        context
            .report()
            .addLine(
                "Invalid BackupReader, not able to construct instance of " + clazz.getSimpleName());
        throw new IllegalArgumentException(
            "Invalid BackupReader: "
                + clazz.getSimpleName()
                + ", need to implement proper constructor: "
                + args,
            e);
      }
      EbeanAspectBackupIterator<ParquetReaderWrapper> iterator =
          backupReader.getBackupIterator(context);
      ParquetReaderWrapper reader;
      List<Future<Integer>> futureList = new ArrayList<>();
      while ((reader = iterator.getNextReader()) != null) {
        final ParquetReaderWrapper readerRef = reader;
        futureList.add(_fileReaderThreadPool.submit(() -> readerExecutable(readerRef, context)));
      }
      for (Future<Integer> future : futureList) {
        try {
          numRows = numRows + future.get();
        } catch (InterruptedException | ExecutionException e) {
          context.report().addLine("Reading interrupted, not able to finish processing.");
          throw new RuntimeException(e);
        }
      }

      context
          .report()
          .addLine(
              String.format(
                  "Added %d rows to the aspect v2 table, took %s ms",
                  numRows, System.currentTimeMillis() - initialStartTime));
      context
          .report()
          .addLine(
              "Entity counts: "
                  + _entityCounts.entrySet().stream()
                      .map(entry -> entry.getKey() + "->" + entry.getValue().get())
                      .collect(Collectors.joining("\n\t")));
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private Integer readerExecutable(ParquetReaderWrapper reader, UpgradeContext context)
      throws ExecutionException, InterruptedException {

    EbeanAspectV2 aspect;
    long startTime = System.currentTimeMillis();
    log.info("Processing file {}", reader.getFileName());
    int numRows = 0;
    Map<String, String> entityUrnMap = new HashMap<>();
    while ((aspect = reader.next()) != null) {
      if (aspect.getVersion() != 0) {
        continue;
      }
      numRows++;

      if (Boolean.parseBoolean(System.getenv(RestoreIndices.DRY_RUN))) {
        if (numRows % 100 == 0) {
          context
              .report()
              .addLine(
                  String.format(
                      "Dry run enabled, continuing. Took %s ms to read %s aspects from parquet.",
                      System.currentTimeMillis() - startTime, 100));
          startTime = System.currentTimeMillis();
        }
      }

      // 1. Extract an Entity type from the entity Urn
      Urn urn;
      try {
        urn = Urn.createFromString(aspect.getKey().getUrn());
      } catch (Exception e) {
        context
            .report()
            .addLine(
                String.format(
                    "Failed to bind Urn with value %s into Urn object: %s. Ignoring row.",
                    aspect.getKey().getUrn(), e));
        continue;
      }

      // 2. Verify that the entity associated with the aspect is found in the registry.
      final String entityName = urn.getEntityType();
      final EntitySpec entitySpec;
      try {
        entitySpec = systemOperationContext.getEntityRegistry().getEntitySpec(entityName);
      } catch (Exception e) {
        context
            .report()
            .addLine(
                String.format(
                    "Failed to find entity with name %s in Entity Registry: %s. Ignoring row.",
                    entityName, e));
        continue;
      }
      final String aspectName = aspect.getKey().getAspect();

      // 3. Verify that the aspect is a valid aspect associated with the entity
      AspectSpec aspectSpec = entitySpec.getAspectSpec(aspectName);
      if (aspectSpec == null) {
        context
            .report()
            .addLine(
                String.format(
                    "Failed to find aspect with name %s associated with entity named %s",
                    aspectName, entityName));
        continue;
      }

      // 4. Create record from json aspect
      final SystemAspect systemAspectRecord;
      try {
        boolean createDefaultAspects =
            context
                .parsedArgs()
                .get(CREATE_DEFAULT_ASPECTS_ARG_NAME)
                .map(Boolean::parseBoolean)
                .orElse(false);
        systemAspectRecord =
            EntityUtils.toSystemAspectFromEbeanAspects(
                    systemOperationContext.getRetrieverContext(),
                    List.of(aspect),
                    createDefaultAspects)
                .get(0);
      } catch (Exception e) {
        context
            .report()
            .addLine(
                String.format(
                    "Failed to deserialize row %s for entity %s, aspect %s: %s. Ignoring row.",
                    aspect.getMetadata(), entityName, aspectName, e));
        continue;
      }

      // 5. Produce MAE events for the aspect record
      _entityService
          .alwaysProduceMCLAsync(
              systemOperationContext,
              urn,
              entityName,
              aspectName,
              aspectSpec,
              null,
              systemAspectRecord.getRecordTemplate(),
              null,
              systemAspectRecord.getSystemMetadata(),
              new AuditStamp()
                  .setActor(UrnUtils.getUrn(SYSTEM_ACTOR))
                  .setTime(System.currentTimeMillis()),
              ChangeType.RESTATE)
          .getFirst()
          .get();

      try {
        this._entityCounts.compute(
            entityName,
            (key, count) -> {
              if (count == null) {
                return new AtomicInteger(1);
              } else {
                // Update data, this part its ok!
                count.incrementAndGet();
                return count;
              }
            });
      } catch (Exception e) {

      }
      entityUrnMap.put(entityName, urn.toString());
    }
    _numRows.addAndGet(numRows);
    String entityUrnString =
        entityUrnMap.entrySet().stream()
            .map(entry -> entry.getKey() + "->" + entry.getValue())
            .collect(Collectors.joining(","));
    context
        .report()
        .addLine(
            String.format(
                "Took %s ms to produce %s MCLs.", System.currentTimeMillis() - startTime, numRows));
    log.info("Latest urns: {}", entityUrnString);

    return numRows;
  }
}
