package com.linkedin.datahub.upgrade.nocode;

import com.datahub.util.RecordUtils;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV1;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.PegasusUtils;
import com.linkedin.util.Pair;
import io.ebean.Database;
import io.ebean.PagedList;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class DataMigrationStep implements UpgradeStep {

  private static final int DEFAULT_BATCH_SIZE = 1000;
  private static final long DEFAULT_BATCH_DELAY_MS = 250;

  private static final String BROWSE_PATHS_ASPECT_NAME =
      PegasusUtils.getAspectNameFromSchema(new BrowsePaths().schema());

  private final Database _server;
  private final EntityService _entityService;
  private final EntityRegistry _entityRegistry;
  private final Set<Urn> urnsWithBrowsePath = new HashSet<>();

  public DataMigrationStep(
      final Database server,
      final EntityService entityService,
      final EntityRegistry entityRegistry) {
    _server = server;
    _entityService = entityService;
    _entityRegistry = entityRegistry;
  }

  @Override
  public String id() {
    return "DataMigrationStep";
  }

  @Override
  public int retryCount() {
    return 0;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      context.report().addLine("Starting data migration...");
      final int rowCount = _server.find(EbeanAspectV1.class).findCount();
      context.report().addLine(String.format("Found %s rows in legacy aspects table", rowCount));

      int totalRowsMigrated = 0;
      int start = 0;
      int count = getBatchSize(context.parsedArgs());
      while (start < rowCount) {

        context
            .report()
            .addLine(
                String.format(
                    "Reading rows %s through %s from legacy aspects table.", start, start + count));
        PagedList<EbeanAspectV1> rows = getPagedAspects(start, count);

        for (EbeanAspectV1 oldAspect : rows.getList()) {

          final String oldAspectName = oldAspect.getKey().getAspect();

          // 1. Instantiate the RecordTemplate class associated with the aspect.
          final RecordTemplate aspectRecord;
          try {
            aspectRecord =
                RecordUtils.toRecordTemplate(
                    Class.forName(oldAspectName).asSubclass(RecordTemplate.class),
                    oldAspect.getMetadata());
          } catch (Exception e) {
            context
                .report()
                .addLine(
                    String.format(
                        "Failed to convert aspect with name %s into a RecordTemplate class",
                        oldAspectName),
                    e);
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // 2. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(oldAspect.getKey().getUrn());
          } catch (Exception e) {
            throw new RuntimeException(
                String.format(
                    "Failed to bind Urn with value %s into Urn object",
                    oldAspect.getKey().getUrn()),
                e);
          }

          // 3. Verify that the entity associated with the aspect is found in the registry.
          final String entityName = urn.getEntityType();
          final EntitySpec entitySpec;
          try {
            entitySpec = _entityRegistry.getEntitySpec(entityName);
          } catch (Exception e) {
            context
                .report()
                .addLine(
                    String.format(
                        "Failed to find Entity with name %s in Entity Registry", entityName),
                    e);
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // 4. Extract new aspect name from Aspect schema
          final String newAspectName;
          try {
            newAspectName = PegasusUtils.getAspectNameFromSchema(aspectRecord.schema());
          } catch (Exception e) {
            context
                .report()
                .addLine(
                    String.format(
                        "Failed to retrieve @Aspect name from schema %s, urn %s",
                        aspectRecord.schema().getFullName(), entityName),
                    e);
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // 5. Verify that the aspect is a valid aspect associated with the entity
          AspectSpec aspectSpec;
          try {
            aspectSpec = entitySpec.getAspectSpec(newAspectName);
          } catch (Exception e) {
            context
                .report()
                .addLine(
                    String.format(
                        "Failed to find aspect spec with name %s associated with entity named %s",
                        newAspectName, entityName),
                    e);
            return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
          }

          // 6. Write the row back using the EntityService
          boolean emitMae = oldAspect.getKey().getVersion() == 0L;
          _entityService.ingestAspects(
              urn, List.of(Pair.of(newAspectName, aspectRecord)), toAuditStamp(oldAspect), null);

          // 7. If necessary, emit a browse path aspect.
          if (entitySpec.getAspectSpecMap().containsKey(BROWSE_PATHS_ASPECT_NAME)
              && !urnsWithBrowsePath.contains(urn)) {
            // Emit a browse path aspect.
            final BrowsePaths browsePaths;
            try {
              browsePaths = _entityService.buildDefaultBrowsePath(urn);

              final AuditStamp browsePathsStamp = new AuditStamp();
              browsePathsStamp.setActor(Urn.createFromString(Constants.SYSTEM_ACTOR));
              browsePathsStamp.setTime(System.currentTimeMillis());

              _entityService.ingestAspects(
                  urn,
                  List.of(Pair.of(BROWSE_PATHS_ASPECT_NAME, browsePaths)),
                  browsePathsStamp,
                  null);
              urnsWithBrowsePath.add(urn);

            } catch (URISyntaxException e) {
              throw new RuntimeException("Failed to ingest Browse Path", e);
            }
          }

          totalRowsMigrated++;
        }
        context.report().addLine(String.format("Successfully migrated %s rows", totalRowsMigrated));
        start = start + count;
        try {
          TimeUnit.MILLISECONDS.sleep(getBatchDelayMs(context.parsedArgs()));
        } catch (InterruptedException e) {
          throw new RuntimeException(
              "Thread interrupted while sleeping after successful batch migration.");
        }
      }
      if (totalRowsMigrated != rowCount) {
        context
            .report()
            .addLine(
                String.format(
                    "Number of rows migrated %s does not equal the number of input rows %s...",
                    totalRowsMigrated, rowCount));
        return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.FAILED);
      }
      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private AuditStamp toAuditStamp(final EbeanAspectV1 aspect) {
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

  private PagedList<EbeanAspectV1> getPagedAspects(final int start, final int pageSize) {
    return _server
        .find(EbeanAspectV1.class)
        .select(EbeanAspectV1.ALL_COLUMNS)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .findPagedList();
  }

  private int getBatchSize(final Map<String, Optional<String>> parsedArgs) {
    int resolvedBatchSize = DEFAULT_BATCH_SIZE;
    if (parsedArgs.containsKey(NoCodeUpgrade.BATCH_SIZE_ARG_NAME)
        && parsedArgs.get(NoCodeUpgrade.BATCH_SIZE_ARG_NAME).isPresent()) {
      resolvedBatchSize = Integer.parseInt(parsedArgs.get(NoCodeUpgrade.BATCH_SIZE_ARG_NAME).get());
    }
    return resolvedBatchSize;
  }

  private long getBatchDelayMs(final Map<String, Optional<String>> parsedArgs) {
    long resolvedBatchDelayMs = DEFAULT_BATCH_DELAY_MS;
    if (parsedArgs.containsKey(NoCodeUpgrade.BATCH_DELAY_MS_ARG_NAME)
        && parsedArgs.get(NoCodeUpgrade.BATCH_DELAY_MS_ARG_NAME).isPresent()) {
      resolvedBatchDelayMs =
          Long.parseLong(parsedArgs.get(NoCodeUpgrade.BATCH_DELAY_MS_ARG_NAME).get());
    }
    return resolvedBatchDelayMs;
  }
}
