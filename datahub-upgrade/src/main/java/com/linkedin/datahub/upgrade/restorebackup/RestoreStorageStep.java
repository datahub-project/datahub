package com.linkedin.datahub.upgrade.restorebackup;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.BackupReader;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.EbeanAspectBackupIterator;
import com.linkedin.datahub.upgrade.restorebackup.backupreader.LocalParquetReader;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;


public class RestoreStorageStep implements UpgradeStep {

  private static final int REPORT_BATCH_SIZE = 1000;

  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;
  private final Map<String, BackupReader> _backupReaders;

  public RestoreStorageStep(final EntityService entityService, final EntityRegistry entityRegistry) {
    _entityService = entityService;
    _entityRegistry = entityRegistry;
    _backupReaders = ImmutableList.of(new LocalParquetReader())
        .stream()
        .collect(Collectors.toMap(BackupReader::getName, Function.identity()));
  }

  @Override
  public String id() {
    return "RestoreStorageStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {

      context.report().addLine("Starting backup restore...");
      int numRows = 0;
      Optional<String> backupReaderName = context.parsedArgs().get("BACKUP_READER");
      if (!backupReaderName.isPresent() || !_backupReaders.containsKey(backupReaderName.get())) {
        context.report().addLine("BACKUP_READER is not set or is not valid");
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }

      EbeanAspectBackupIterator iterator = _backupReaders.get(backupReaderName.get()).getBackupIterator(context);
      EbeanAspectV2 aspect;
      while ((aspect = iterator.next()) != null) {
        numRows++;

        // 1. Extract an Entity type from the entity Urn
        Urn urn;
        try {
          urn = Urn.createFromString(aspect.getKey().getUrn());
        } catch (Exception e) {
          context.report()
              .addLine(
                  String.format("Failed to bind Urn with value %s into Urn object: %s", aspect.getKey().getUrn(), e));
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        }

        // 2. Verify that the entity associated with the aspect is found in the registry.
        final String entityName = urn.getEntityType();
        final EntitySpec entitySpec;
        try {
          entitySpec = _entityRegistry.getEntitySpec(entityName);
        } catch (Exception e) {
          context.report()
              .addLine(String.format("Failed to find Entity with name %s in Entity Registry: %s", entityName, e));
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        }
        final String aspectName = aspect.getKey().getAspect();

        // 3. Create record from json aspect
        final RecordTemplate aspectRecord =
            EntityUtils.toAspectRecord(entityName, aspectName, aspect.getMetadata(), _entityRegistry);

        // 4. Verify that the aspect is a valid aspect associated with the entity
        AspectSpec aspectSpec;
        try {
          aspectSpec = entitySpec.getAspectSpec(aspectName);
        } catch (Exception e) {
          context.report()
              .addLine(String.format("Failed to find aspect spec with name %s associated with entity named %s: %s",
                  aspectName, entityName, e));
          return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
        }

        // 5. Write the row back using the EntityService
        boolean emitMae = aspect.getKey().getVersion() == 0L;
        _entityService.updateAspect(urn, entityName, aspectName, aspectSpec, aspectRecord, toAuditStamp(aspect),
            aspect.getKey().getVersion(), emitMae);

        if (numRows % REPORT_BATCH_SIZE == 0) {
          context.report().addLine(String.format("Successfully inserted %d rows", numRows));
        }
      }

      context.report().addLine(String.format("Added %d rows to the aspect v2 table", numRows));
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private AuditStamp toAuditStamp(final EbeanAspectV2 aspect) {
    final AuditStamp auditStamp = new AuditStamp();
    auditStamp.setTime(aspect.getCreatedOn().getTime());

    try {
      auditStamp.setActor(new Urn(aspect.getCreatedBy()));
      if (aspect.getCreatedFor() != null) {
        auditStamp.setImpersonator(new Urn(aspect.getCreatedFor()));
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return auditStamp;
  }
}
