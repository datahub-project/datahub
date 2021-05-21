package com.linkedin.datahub.upgrade.nocode;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.BrowsePaths;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.metadata.EntitySpecUtils;
import com.linkedin.metadata.dao.utils.RecordUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ebean.EbeanAspectV1;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.metadata.search.indexbuilder.BrowsePathUtils;
import io.ebean.EbeanServer;
import io.ebean.PagedList;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


public class DataMigrationStep implements UpgradeStep<Void> {

  private final EbeanServer _server;
  private final EntityService _entityService;
  private final SnapshotEntityRegistry _entityRegistry;
  private final Set<Urn> urnsWithBrowsePath = new HashSet<>();

  public DataMigrationStep(
      final EbeanServer server,
      final EntityService entityService,
      final SnapshotEntityRegistry entityRegistry) {
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
    return 1;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult<Void>> executable() {
    return (context) -> {

      context.report().addLine("Starting data migration...");

      final int rowCount = _server.find(EbeanAspectV1.class).findCount();

      context.report().addLine(String.format("Found %s rows in legacy aspects table.", rowCount));

      int totalRowsMigrated = 0;
      int totalBatchesMigrated = 0;

      int start = 0;
      int count = 1000;
      while (start < rowCount) {

        context.report().addLine(String.format("Reading rows %s through %s from legacy aspects table.", start, start + count));

        // 1. Get batch of rows
        PagedList<EbeanAspectV1> rows = getPagedAspects(start, count);

        // 2. Convert batch of rows into something more interesting..
        for (EbeanAspectV1 aspect : rows.getList()) {
          final String aspectName = aspect.getKey().getAspect();

          // 3. Instantiate the RecordTemplate class associated with the aspect.
          final RecordTemplate aspectRecord;
          try {
            aspectRecord = RecordUtils.toRecordTemplate(
                Class.forName(aspectName).asSubclass(RecordTemplate.class),
                aspect.getMetadata());
          } catch (Exception e) {
            return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.FAILED, UpgradeStepResult.Action.ABORT,
                String.format("Failed to convert aspect with name %s into a RecordTemplate class: %s", aspectName, e.getMessage()));
          }

          // 4. Extract an Entity type from the entity Urn
          Urn urn;
          try {
            urn = Urn.createFromString(aspect.getKey().getUrn());
          } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to bind Urn with value %s into Urn object", aspect.getKey().getUrn(), e));
          }

          // 5. Verify that the entity associated with the aspect is found
          // in the registry.
          final String entityName = urn.getEntityType();
          final EntitySpec entitySpec;
          try {
            entitySpec = _entityRegistry.getEntitySpec(entityName);
          } catch (Exception e) {
            return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.FAILED, UpgradeStepResult.Action.ABORT,
                String.format("Failed to find Entity with name %s in Entity Registry: %s", entityName, e.toString()));
          }

          // 6. Extract new aspect name from Aspect schema
          final String newAspectName;
          try {
            newAspectName = EntitySpecUtils.getAspectNameFromSchema(aspectRecord.schema());
          } catch (Exception e) {
            return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.FAILED, UpgradeStepResult.Action.ABORT,
                String.format("Failed to retrieve @Aspect name from schema %s, urn %s: %s",
                    aspectRecord.schema().getFullName(),
                    entityName,
                    e.toString()));
          }

          // 7. Verify that the aspect is a valid aspect associated with the entity
          try {
            entitySpec.getAspectSpec(newAspectName);
          } catch (Exception e) {
            return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.FAILED, UpgradeStepResult.Action.ABORT,
                String.format("Failed to find aspect spec with name %s associated with entity named %s",
                    newAspectName,
                    entityName,
                    e.toString()));
          }

          // 8. Write the row back using the EntityService
          boolean emitMae = aspect.getKey().getVersion() == 0L;
          _entityService.updateAspect(
              urn,
              newAspectName,
              aspectRecord,
              toAuditStamp(aspect),
              aspect.getKey().getVersion(),
              emitMae
          );

          // 9. If necessary, emit a browse path aspect.
          if (entitySpec.getAspectSpecMap().containsKey("browsePaths") && !urnsWithBrowsePath.contains(urn)) {
            // Emit a browse path aspect.
            BrowsePaths browsePaths;
            try {
              browsePaths = BrowsePathUtils.buildBrowsePath(urn);

              final AuditStamp browsePathsStamp = new AuditStamp();
              browsePathsStamp.setActor(Urn.createFromString("urn:li:principal:system"));
              browsePathsStamp.setTime(System.currentTimeMillis());

              // Now, ingest the browse path
              _entityService.ingestAspect(urn, "browsePaths", browsePaths, browsePathsStamp);

              urnsWithBrowsePath.add(urn);

            } catch (URISyntaxException e) {
              throw new RuntimeException("Failed to ingest the Browse Path!", e);
            }
          }

          // Keep track of total records migrated.
          totalRowsMigrated++;
        }
        context.report().addLine(String.format("Successfully migrated rows %s rows", totalRowsMigrated));
        totalBatchesMigrated++;
        start = start + count;
        try {
          // TODO: Make batch size and sleep time configurable.
          TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
          throw new RuntimeException("Thread interrupted while sleeping after successful batch migration.");
        }
      }
      return new DefaultUpgradeStepResult<>(id(), UpgradeStepResult.Result.SUCCEEDED);
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
    return _server.find(EbeanAspectV1.class)
        .select(EbeanAspectV1.ALL_COLUMNS)
        .setFirstRow(start)
        .setMaxRows(pageSize)
        .orderBy()
        .asc(EbeanAspectV2.URN_COLUMN)
        .findPagedList();
  }
}
