package com.linkedin.datahub.upgrade.system.assertions;

import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_NOTE_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SYSTEM_ACTOR;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionNote;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.entity.restoreindices.RestoreIndicesArgs;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Upgrade step that copies the deprecated {@code note} field from {@code assertionInfo} into the
 * new dedicated {@code assertionNote} aspect.
 *
 * <p>The step streams all {@code assertionInfo} records directly from SQL, batch-checks for the
 * presence of an existing {@code assertionNote} aspect, and only writes {@code assertionNote} where
 * one does not already exist. This makes the step safe to run multiple times and safe to interrupt
 * (resume picks up from {@code lastUrn}).
 *
 * <p>Individual write failures are logged and skipped rather than aborting the migration — affected
 * assertions retain their notes via the deprecated {@code assertionInfo.note} fallback in {@code
 * AssertionMapper} and can be addressed manually.
 */
@Slf4j
public class MigrateAssertionNoteToAspectStep implements UpgradeStep {

  public static final String STEP_ID = "migrate-assertion-note-to-aspect-v1";
  public static final String LAST_URN_KEY = "lastUrn";

  private final OperationContext opContext;
  private final EntityService<?> entityService;
  private final AspectDao aspectDao;
  private final int batchSize;
  private final int batchDelayMs;
  private final int limit;

  public MigrateAssertionNoteToAspectStep(
      OperationContext opContext,
      EntityService<?> entityService,
      AspectDao aspectDao,
      Integer batchSize,
      Integer batchDelayMs,
      Integer limit) {
    this.opContext = opContext;
    this.entityService = entityService;
    this.aspectDao = aspectDao;
    this.batchSize = batchSize != null ? batchSize : RestoreIndicesArgs.DEFAULT_BATCH_SIZE;
    this.batchDelayMs = batchDelayMs != null ? batchDelayMs : 0;
    this.limit = limit != null ? limit : 0;
    log.info("{} initialized", STEP_ID);
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @VisibleForTesting
  public Urn getUpgradeIdUrn() {
    return BootstrapStep.getUpgradeUrn(id());
  }

  @VisibleForTesting
  public String getUrnLike() {
    return "urn:li:" + ASSERTION_ENTITY_NAME + ":%";
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return (context) -> {
      final AuditStamp auditStamp;
      try {
        auditStamp =
            new AuditStamp()
                .setActor(Urn.createFromString(SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis());
      } catch (URISyntaxException e) {
        throw new RuntimeException("Failed to create system actor urn", e);
      }

      // Resume from last checkpoint if present.
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

      log.info(
          "{}: Starting. batchSize={} delayMs={} limit={} resumeFrom={}",
          STEP_ID,
          batchSize,
          batchDelayMs,
          limit,
          resumeUrn != null ? resumeUrn : "beginning");

      final AtomicLong totalMigrated = new AtomicLong(0);
      final AtomicLong totalSkipped = new AtomicLong(0);
      final AtomicLong totalNoNote = new AtomicLong(0);
      final AtomicLong totalErrors = new AtomicLong(0);
      final AtomicLong batchNumber = new AtomicLong(0);

      RestoreIndicesArgs args =
          new RestoreIndicesArgs()
              .aspectNames(List.of(ASSERTION_INFO_ASPECT_NAME))
              .urnLike(getUrnLike())
              .batchSize(batchSize)
              .lastUrn(resumeUrn)
              .urnBasedPagination(resumeUrn != null)
              .limit(limit);

      aspectDao.streamAspectBatches(
          opContext,
          args,
          stream -> {
            stream
                .partition(args.batchSize)
                .forEach(
                    batch -> {
                      long currentBatch = batchNumber.incrementAndGet();
                      log.info("{}: Processing batch {}", STEP_ID, currentBatch);

                      List<com.linkedin.metadata.aspect.SystemAspect> batchAspects =
                          EntityUtils.toSystemAspectFromEbeanAspects(
                              opContext,
                              opContext.getRetrieverContext(),
                              batch.collect(Collectors.toList()));

                      if (batchAspects.isEmpty()) {
                        return;
                      }

                      // Batch-check which URNs already have assertionNote to avoid overwriting a
                      // note
                      // that was set after the dedicated aspect was introduced.
                      Map<String, Set<String>> urnToAspects =
                          batchAspects.stream()
                              .collect(
                                  Collectors.toMap(
                                      sa -> sa.getUrn().toString(),
                                      sa -> Set.of(ASSERTION_NOTE_ASPECT_NAME),
                                      (a, b) -> a));

                      Map<String, Map<String, com.linkedin.metadata.aspect.SystemAspect>>
                          existingNotes =
                              aspectDao.getLatestAspects(opContext, urnToAspects, false);

                      // Collect proposals for assertions that need migration.
                      MigrationCounts counts = new MigrationCounts(totalNoNote, totalSkipped);
                      List<com.linkedin.mxe.MetadataChangeProposal> proposals =
                          buildMigrationProposals(batchAspects, existingNotes, counts);

                      if (!proposals.isEmpty()) {
                        log.info(
                            "{}: Writing {} assertionNote aspects in batch {}",
                            STEP_ID,
                            proposals.size(),
                            currentBatch);
                        for (com.linkedin.mxe.MetadataChangeProposal proposal : proposals) {
                          try {
                            entityService.ingestProposal(opContext, proposal, auditStamp, false);
                            totalMigrated.incrementAndGet();
                          } catch (Exception e) {
                            // A single bad assertion should not abort the entire migration.
                            // Affected
                            // assertions retain their notes via the assertionInfo.note fallback.
                            log.warn(
                                "{}: Failed to migrate assertionNote for {}, skipping",
                                STEP_ID,
                                proposal.getEntityUrn(),
                                e);
                            totalErrors.incrementAndGet();
                          }
                        }
                      }

                      // Checkpoint after each batch for resume capability.
                      com.linkedin.metadata.aspect.SystemAspect lastAspect =
                          batchAspects.stream().reduce((a, b) -> b).orElse(null);
                      if (lastAspect != null) {
                        log.info(
                            "{}: Batch {} done. lastUrn={} migrated={} skipped={} noNote={} errors={}",
                            STEP_ID,
                            currentBatch,
                            lastAspect.getUrn(),
                            totalMigrated.get(),
                            totalSkipped.get(),
                            totalNoNote.get(),
                            totalErrors.get());
                        context
                            .upgrade()
                            .setUpgradeResult(
                                opContext,
                                getUpgradeIdUrn(),
                                entityService,
                                DataHubUpgradeState.IN_PROGRESS,
                                Map.of(LAST_URN_KEY, lastAspect.getUrn().toString()));
                      }

                      if (batchDelayMs > 0) {
                        try {
                          Thread.sleep(batchDelayMs);
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException("Migration interrupted during batch delay", e);
                        }
                      }
                    });
            return null;
          });

      log.info(
          "{}: Migration complete. migrated={} skipped={} noNote={} errors={}",
          STEP_ID,
          totalMigrated.get(),
          totalSkipped.get(),
          totalNoNote.get(),
          totalErrors.get());
      if (totalErrors.get() > 0) {
        log.warn(
            "{}: {} assertion(s) could not be migrated. They retain notes via the deprecated"
                + " assertionInfo.note fallback and can be retried manually.",
            STEP_ID,
            totalErrors.get());
      }

      BootstrapStep.setUpgradeResult(opContext, getUpgradeIdUrn(), entityService);
      context.report().addLine("State updated: " + getUpgradeIdUrn());
      context
          .report()
          .addLine(
              String.format(
                  "Migration summary: migrated=%d skipped=%d noNote=%d errors=%d",
                  totalMigrated.get(), totalSkipped.get(), totalNoNote.get(), totalErrors.get()));

      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    };
  }

  /**
   * Pure filtering logic: given a batch of assertionInfo aspects and the map of pre-fetched
   * existing assertionNote aspects, returns only the MCPs that need to be written.
   *
   * <p>Extracted for testability — callers can supply arbitrary inputs without going through the
   * SQL streaming path.
   */
  @VisibleForTesting
  public List<com.linkedin.mxe.MetadataChangeProposal> buildMigrationProposals(
      List<com.linkedin.metadata.aspect.SystemAspect> batchAspects,
      Map<String, Map<String, com.linkedin.metadata.aspect.SystemAspect>> existingNotes,
      MigrationCounts counts) {
    List<com.linkedin.mxe.MetadataChangeProposal> proposals = new ArrayList<>();
    for (com.linkedin.metadata.aspect.SystemAspect infoAspect : batchAspects) {
      AssertionInfo assertionInfo = new AssertionInfo(infoAspect.getRecordTemplate().data());
      if (!assertionInfo.hasNote()) {
        counts.noNote.incrementAndGet();
        continue;
      }

      String urnStr = infoAspect.getUrn().toString();
      Map<String, com.linkedin.metadata.aspect.SystemAspect> existing =
          existingNotes.getOrDefault(urnStr, Map.of());
      if (existing.containsKey(ASSERTION_NOTE_ASPECT_NAME)) {
        log.debug("{}: Skipping {}, assertionNote already exists", STEP_ID, urnStr);
        counts.skipped.incrementAndGet();
        continue;
      }

      AssertionNote noteAspect = assertionInfo.getNote();
      log.debug("{}: Queuing note migration for {}", STEP_ID, urnStr);
      proposals.add(
          AspectUtils.buildMetadataChangeProposal(
              infoAspect.getUrn(), ASSERTION_NOTE_ASPECT_NAME, noteAspect));
    }
    return proposals;
  }

  /** Mutable counters passed into {@link #buildMigrationProposals} so totals stay consistent. */
  @VisibleForTesting
  public static class MigrationCounts {
    public final AtomicLong noNote;
    public final AtomicLong skipped;

    public MigrationCounts(AtomicLong noNote, AtomicLong skipped) {
      this.noNote = noNote;
      this.skipped = skipped;
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
}
