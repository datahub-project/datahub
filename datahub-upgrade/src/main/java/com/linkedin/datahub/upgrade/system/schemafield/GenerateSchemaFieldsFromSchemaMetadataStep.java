package com.linkedin.datahub.upgrade.system.schemafield;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static com.linkedin.metadata.Constants.SCHEMA_METADATA_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.ebean.batch.AspectsBatchImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

/**
 * The `GenerateSchemaFieldsFromSchemaMetadataStep` class is an implementation of the `UpgradeStep`
 * interface. This class is responsible for generating schema fields from schema metadata during an
 * upgrade process.
 *
 * <p>The step performs the following actions: 1. Initializes with provided operation context,
 * entity service, and aspect DAO. 2. Provides a unique identifier for the upgrade step. 3.
 * Determines if the upgrade should be skipped based on the environment variable. 4. Executes the
 * upgrade step which involves streaming aspects in batches, processing them, and updating schema
 * fields.
 *
 * <p>This class utilizes various metadata and entity services to perform its operations, and
 * includes configuration parameters such as batch size, delay between batches, and limits.
 *
 * <p>Environment Variables: - `SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA`: If set to `true`,
 * the upgrade step is skipped.
 *
 * <p>Note: Schema Fields are generated with a status aspect to indicate presence of the field. No
 * tags, documentation or other aspects are generated. We will write an upgrade to this job to
 * generate the other aspects in the future (v2).
 */
@Slf4j
public class GenerateSchemaFieldsFromSchemaMetadataStep implements UpgradeStep {
  private static final List<String> REQUIRED_ASPECTS =
      List.of(SCHEMA_METADATA_ASPECT_NAME, STATUS_ASPECT_NAME);

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;

  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public GenerateSchemaFieldsFromSchemaMetadataStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
    log.info("GenerateSchemaFieldsFromSchemaMetadataStep initialized");
  }

  @Override
  public String id() {
    return "schema-field-from-schema-metadata-v1";
  }

  @VisibleForTesting
  @Nullable
  public String getUrnLike() {
    return "urn:li:" + DATASET_ENTITY_NAME + ":%";
  }

  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    if (Boolean.parseBoolean(System.getenv("SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA"))) {
      log.info(
          "Environment variable SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA is set to true. Skipping.");
      return true;
    }

    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    return prevResult
        .filter(
            result ->
                DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                    || DataHubUpgradeState.ABORTED.equals(result.getState()))
        .isPresent();
  }

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting GenerateSchemaFieldsFromSchemaMetadataStep");
    return (context) -> {
      // Resume state
      Optional<DataHubUpgradeResult> prevResult =
          context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      String resumeUrn =
          prevResult
              .filter(
                  result ->
                      DataHubUpgradeState.IN_PROGRESS.equals(result.getState())
                          && result.getResult() != null
                          && result.getResult().containsKey(LAST_URN_KEY))
              .map(result -> result.getResult().get(LAST_URN_KEY))
              .orElse(null);
      if (resumeUrn != null) {
        log.info("{}: Resuming from URN: {}", getUpgradeIdUrn(), resumeUrn);
      }

      // re-using for configuring the sql scan
      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectNames(REQUIRED_ASPECTS)
              .batchSize(batchSize)
              .lastUrn(resumeUrn)
              .urnBasedPagination(resumeUrn != null)
              .limit(limit);

      if (getUrnLike() != null) {
        args = args.urnLike(getUrnLike());
      }

      try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(args)) {
        stream
            .partition(args.batchSize)
            .forEach(
                batch -> {
                  log.info("Processing batch of size {}.", batchSize);

                  AspectsBatch aspectsBatch =
                      AspectsBatchImpl.builder()
                          .retrieverContext(opContext.getRetrieverContext())
                          .items(
                              batch
                                  .flatMap(
                                      ebeanAspectV2 ->
                                          EntityUtils.toSystemAspectFromEbeanAspects(
                                              opContext.getRetrieverContext(),
                                              Set.of(ebeanAspectV2))
                                              .stream())
                                  .map(
                                      systemAspect ->
                                          ChangeItemImpl.builder()
                                              .changeType(ChangeType.UPSERT)
                                              .urn(systemAspect.getUrn())
                                              .entitySpec(systemAspect.getEntitySpec())
                                              .aspectName(systemAspect.getAspectName())
                                              .aspectSpec(systemAspect.getAspectSpec())
                                              .recordTemplate(systemAspect.getRecordTemplate())
                                              .auditStamp(systemAspect.getAuditStamp())
                                              .systemMetadata(
                                                  withAppSource(systemAspect.getSystemMetadata()))
                                              .build(opContext.getAspectRetriever()))
                                  .collect(Collectors.toList()))
                          .build();

                  // re-ingest the aspects to trigger side effects
                  entityService.ingestAspects(opContext, aspectsBatch, true, false);

                  // record progress
                  Urn lastUrn =
                      aspectsBatch.getItems().stream()
                          .reduce((a, b) -> b)
                          .map(ReadItem::getUrn)
                          .orElse(null);
                  if (lastUrn != null) {
                    log.info("{}: Saving state. Last urn:{}", getUpgradeIdUrn(), lastUrn);
                    context
                        .upgrade()
                        .setUpgradeResult(
                            opContext,
                            getUpgradeIdUrn(),
                            entityService,
                            DataHubUpgradeState.IN_PROGRESS,
                            Map.of(LAST_URN_KEY, lastUrn.toString()));
                  }

                  if (batchDelayMs > 0) {
                    log.info("Sleeping for {} ms", batchDelayMs);
                    try {
                      Thread.sleep(batchDelayMs);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
      }

      BootstrapStep.setUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      context.report().addLine("State updated: " + getUpgradeIdUrn());

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private static SystemMetadata withAppSource(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata withAppSourceSystemMetadata = null;
    try {
      withAppSourceSystemMetadata =
          systemMetadata != null
              ? new SystemMetadata(systemMetadata.copy().data())
              : new SystemMetadata();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    StringMap properties = withAppSourceSystemMetadata.getProperties();
    StringMap map = properties != null ? new StringMap(properties.data()) : new StringMap();
    map.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);

    withAppSourceSystemMetadata.setProperties(map);
    return withAppSourceSystemMetadata;
  }
}
