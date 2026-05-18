package com.linkedin.datahub.upgrade.system.criterion;

import static com.linkedin.datahub.upgrade.system.AbstractMCLStep.LAST_URN_KEY;
import static com.linkedin.metadata.Constants.APP_SOURCE;
import static com.linkedin.metadata.Constants.DATAHUB_VIEW_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_UPDATE_SOURCE;
import static com.linkedin.metadata.aspect.validation.ConditionalWriteValidator.HTTP_HEADER_IF_VERSION_MATCH;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.ReadItem;
import com.linkedin.metadata.aspect.SystemAspect;
import com.linkedin.metadata.aspect.batch.AspectsBatch;
import com.linkedin.metadata.aspect.hooks.migrations.criterion.CriterionFilterMutatorBase;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * Blocking sweep over {@code dataHubViewInfo} and {@code dynamicFormAssignment} using {@link
 * RestoreIndicesArgs#aspectNames} and {@link AspectDao#streamAspectBatches}, matching {@link
 * com.linkedin.datahub.upgrade.system.schemafield.GenerateSchemaFieldsFromSchemaMetadataStep}.
 * Copies and normalizes payloads via {@link CriterionFilterMutatorBase} (legacy {@code
 * Criterion.value} → {@code values}), then builds an {@link AspectsBatch} and calls {@link
 * com.linkedin.metadata.entity.EntityService#ingestAspects} so corrected payloads are written to
 * storage (ChangeMCP / local DB) and MCLs are emitted — unlike {@link
 * com.linkedin.datahub.upgrade.system.AbstractMCLStep}, which only calls {@code
 * alwaysProduceMCLAsync}. Conditional-write headers are preserved on each {@link ChangeItemImpl}.
 * Resume uses {@link com.linkedin.datahub.upgrade.system.AbstractMCLStep#LAST_URN_KEY} like other
 * {@code RestoreIndicesArgs} scans.
 */
@Slf4j
public class CriterionFilterAspectsBlockingStep implements UpgradeStep {

  private static final List<String> SCAN_ASPECT_NAMES =
      List.of(DATAHUB_VIEW_INFO_ASPECT_NAME, DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME);

  static final String STEP_ID_PREFIX = "criterion-filter-aspects-blocking-";

  public static String stepId(@Nonnull String upgradeVersion) {
    return STEP_ID_PREFIX + upgradeVersion;
  }

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final String stepId;
  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public CriterionFilterAspectsBlockingStep(
      @Nonnull OperationContext opContext,
      @Nonnull EntityService<?> entityService,
      @Nonnull AspectDao aspectDao,
      @Nonnull String upgradeVersion,
      int batchSize,
      int batchDelayMs,
      int limit) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.stepId = stepId(upgradeVersion);
    this.batchSize = batchSize;
    this.batchDelayMs = batchDelayMs;
    this.limit = limit;
  }

  @Override
  public String id() {
    return stepId;
  }

  @VisibleForTesting
  protected com.linkedin.common.urn.Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  /**
   * Blocking upgrade only: deep-copy supported aspect payloads and normalize criterion fields via
   * {@link CriterionFilterMutatorBase} before {@link EntityService#ingestAspects}.
   */
  static SystemAspect prepareSystemAspectForBlockingIngest(@Nonnull SystemAspect sa) {
    if (!CriterionFilterMutatorBase.isSupportedAspect(sa.getAspectName())) {
      return sa;
    }
    RecordTemplate rt = sa.getRecordTemplate();
    if (rt == null) {
      return sa;
    }
    try {
      RecordTemplate rtCopy = rt.copy();
      CriterionFilterMutatorBase.sanitizeInPlaceOnCopy(sa.getAspectName(), rtCopy);
      return sa.copy().setRecordTemplate(rtCopy);
    } catch (CloneNotSupportedException e) {
      throw new IllegalStateException(
          "Criterion filter blocking upgrade: failed to copy aspect payload", e);
    }
  }

  @Override
  public boolean skip(UpgradeContext context) {
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, getUpgradeIdUrn(), entityService);

    boolean previousRunFinal =
        prevResult
            .filter(
                result ->
                    DataHubUpgradeState.SUCCEEDED.equals(result.getState())
                        || DataHubUpgradeState.ABORTED.equals(result.getState()))
            .isPresent();

    if (previousRunFinal) {
      log.info(
          "{} was already run. State: {} Skipping.",
          id(),
          prevResult.map(DataHubUpgradeResult::getState));
    }
    return previousRunFinal;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    log.info(
        "{}: Starting criterion-filter blocking sweep for aspect names {}.",
        id(),
        SCAN_ASPECT_NAMES);
    return context -> {
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

      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectNames(SCAN_ASPECT_NAMES)
              .batchSize(batchSize)
              .lastUrn(resumeUrn)
              .urnBasedPagination(resumeUrn != null)
              .limit(limit);

      java.util.concurrent.atomic.AtomicLong totalIngested =
          new java.util.concurrent.atomic.AtomicLong(0);

      try (PartitionedStream<EbeanAspectV2> stream = aspectDao.streamAspectBatches(args)) {
        stream
            .partition(args.batchSize)
            .forEach(
                rawBatch -> {
                  List<EbeanAspectV2> batchList = rawBatch.collect(Collectors.toList());

                  List<ChangeItemImpl> items =
                      batchList.stream()
                          .flatMap(
                              ea ->
                                  EntityUtils.toSystemAspectFromEbeanAspects(
                                      opContext.getRetrieverContext(), Set.of(ea))
                                      .stream())
                          .map(
                              CriterionFilterAspectsBlockingStep
                                  ::prepareSystemAspectForBlockingIngest)
                          .map(
                              prepared ->
                                  ChangeItemImpl.builder()
                                      .changeType(ChangeType.UPSERT)
                                      .urn(prepared.getUrn())
                                      .entitySpec(prepared.getEntitySpec())
                                      .aspectName(prepared.getAspectName())
                                      .aspectSpec(prepared.getAspectSpec())
                                      .recordTemplate(prepared.getRecordTemplate())
                                      .auditStamp(prepared.getAuditStamp())
                                      .systemMetadata(withAppSource(prepared.getSystemMetadata()))
                                      .headers(
                                          versionHeaders(prepared.getSystemMetadata().getVersion()))
                                      .build(opContext.getAspectRetriever()))
                          .collect(Collectors.toList());

                  AspectsBatch aspectsBatch =
                      AspectsBatchImpl.builder()
                          .retrieverContext(opContext.getRetrieverContext())
                          .items(items)
                          .build(opContext);

                  if (!aspectsBatch.getItems().isEmpty()) {
                    // Re-ingest like GenerateSchemaFieldsFromSchemaMetadataStep: persist MCPs to
                    // local DB then emit MCLs (emitMCL=true, overwrite=false).
                    entityService.ingestAspects(opContext, aspectsBatch, true, false);
                    totalIngested.addAndGet(aspectsBatch.getItems().size());
                    log.info(
                        "{}: Re-ingested {} aspect row(s) in batch (DB + MCL).",
                        id(),
                        items.size());
                  }

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
                  } else if (!batchList.isEmpty()) {
                    String lastUrnStr = batchList.get(batchList.size() - 1).getKey().getUrn();
                    log.info("{}: Saving state. Last urn: {}", getUpgradeIdUrn(), lastUrnStr);
                    context
                        .upgrade()
                        .setUpgradeResult(
                            opContext,
                            getUpgradeIdUrn(),
                            entityService,
                            DataHubUpgradeState.IN_PROGRESS,
                            Map.of(LAST_URN_KEY, lastUrnStr));
                  }

                  if (batchDelayMs > 0) {
                    try {
                      Thread.sleep(batchDelayMs);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                  }
                });
      }

      log.info("{}: Sweep complete. Total aspect rows re-ingested: {}.", id(), totalIngested.get());
      BootstrapStep.setUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      context.report().addLine("State updated: " + getUpgradeIdUrn());
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  private static Map<String, String> versionHeaders(@Nullable String version) {
    if (version == null) {
      return Map.of();
    }
    Map<String, String> headers = new HashMap<>();
    headers.put(HTTP_HEADER_IF_VERSION_MATCH, version);
    return headers;
  }

  private static SystemMetadata withAppSource(@Nullable SystemMetadata systemMetadata) {
    SystemMetadata result;
    try {
      result =
          systemMetadata != null
              ? new SystemMetadata(systemMetadata.copy().data())
              : new SystemMetadata();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    StringMap properties = result.getProperties();
    StringMap map = properties != null ? new StringMap(properties.data()) : new StringMap();
    map.put(APP_SOURCE, SYSTEM_UPDATE_SOURCE);
    result.setProperties(map);
    return result;
  }
}
