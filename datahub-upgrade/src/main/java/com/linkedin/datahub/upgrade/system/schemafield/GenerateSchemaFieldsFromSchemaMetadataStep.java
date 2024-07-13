package com.linkedin.datahub.upgrade.system.schemafield;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.UpdateAspectResult;
import com.linkedin.metadata.entity.ebean.EbeanAspectV2;
import com.linkedin.metadata.entity.ebean.PartitionedStream;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.metadata.key.SchemaFieldKey;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.schema.SchemaField;
import com.linkedin.schema.SchemaMetadata;
import com.linkedin.util.Pair;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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

  @Nullable
  protected String getUrnLike() {
    return "urn:li:" + DATASET_ENTITY_NAME + ":%";
  }

  /**
   * Returns whether the upgrade should be skipped. Uses previous run history or the environment
   * variable SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA to determine whether to skip.
   */
  public boolean skip(UpgradeContext context) {
    boolean envFlagRecommendsSkip =
        Boolean.parseBoolean(System.getenv("SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA"));
    if (envFlagRecommendsSkip) {
      log.info(
          "Environment variable SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA is set to true. Skipping.");
    }
    return envFlagRecommendsSkip;
  }

  protected Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info("Starting GenerateSchemaFieldsFromSchemaMetadataStep");
    return (context) -> {

      // re-using for configuring the sql scan
      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectName(SCHEMA_METADATA_ASPECT_NAME)
              .batchSize(batchSize)
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

                  List<List<UpdateAspectResult>> results;

                  results =
                      EntityUtils.toSystemAspectFromEbeanAspects(
                              opContext.getRetrieverContext().get(),
                              batch.collect(Collectors.toList()))
                          .stream()
                          .flatMap(
                              systemAspect ->
                                  shredAspectToSchemaFieldMCPSAsync(opContext, systemAspect))
                          .toList();

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

      return new DefaultUpgradeStepResult(id(), UpgradeStepResult.Result.SUCCEEDED);
    };
  }

  private Stream<List<UpdateAspectResult>> shredAspectToSchemaFieldMCPSAsync(
      OperationContext opContext, SystemAspect systemAspect) {
    if (systemAspect.getAspectName().equals(SCHEMA_METADATA_ASPECT_NAME)) {
      SchemaMetadata schemaMetadata = systemAspect.getAspect(SchemaMetadata.class);
      if (schemaMetadata != null) {
        return schemaMetadata.getFields().stream()
            .map(
                field -> {
                  return entityService.ingestAspects(
                      opContext,
                      getSchemaFieldUrn(systemAspect.getUrn(), field),
                      List.of(
                          Pair.of(
                              "schemaFieldKey",
                              new SchemaFieldKey()
                                  .setFieldPath(field.getFieldPath())
                                  .setParent(systemAspect.getUrn()))),
                      AuditStampUtils.createDefaultAuditStamp(),
                      systemAspect.getSystemMetadata());
                });
      }
    }
    return Stream.empty();
  }

  @VisibleForTesting
  Urn getSchemaFieldUrn(Urn parentUrn, SchemaField field) {
    return Urn.createFromTuple(
        SCHEMA_FIELD_ENTITY_NAME, parentUrn.toString(), field.getFieldPath());
  }
}
