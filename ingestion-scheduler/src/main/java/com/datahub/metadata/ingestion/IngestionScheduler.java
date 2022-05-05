package com.datahub.metadata.ingestion;

import com.datahub.authentication.Authentication;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.IngestionConfiguration;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.support.CronSequenceGenerator;


/**
 * This class serves as a stateful scheduler of Ingestion Runs for Ingestion Sources defined
 * within DataHub. It manages storing and triggering ingestion sources on a pre-defined schedule
 * based on the information present in the {@link DataHubIngestionSourceInfo} aspect. As such, this class
 * should never be instantiated more than once - it's a singleton.
 *
 * When the scheduler is created, it will first batch load all "info" aspects associated with the DataHubIngestionSource entity.
 * It then iterates through all the aspects and attempts to extract a Quartz-cron (* * * * *) formatted schedule string & timezone from each.
 * Upon finding a schedule and timezone, the "next execution time" as a relative timestamp is computed and a task
 * is scheduled at that time in the future.
 *
 * The child task is scheduled on another thread via {@link ScheduledExecutorService} and is responsible for creating a
 * new DataHubExecutionRequest entity instance using an {@link EntityClient}. The execution request includes the inputs required
 * to execute an ingestion source: an Ingestion Recipe encoded as JSON. This in turn triggers the execution of a downstream
 * "action" which actually executes the ingestion process and reports the status back.
 *
 * After initial load, this class will continuously listen to the MetadataChangeProposal stream and update its local cache based
 * on changes performed against Ingestion Source entities. Specifically, if the schedule of an Ingestion Source is changed in any way,
 * the next execution time of that source will be recomputed, with previously scheduled execution clear if necessary.
 *
 * On top of that, the component can also refresh its entire cache periodically. By default, it batch loads all the latest
 * schedules on a once-per-day cadence.
 */
@Slf4j
@RequiredArgsConstructor
public class IngestionScheduler {

  private final Authentication _systemAuthentication;
  private final EntityClient _entityClient;

  // Maps a DataHubIngestionSource to a future representing the "next" scheduled execution of the source
  // Visible for testing
  final Map<Urn, ScheduledFuture<?>> _nextIngestionSourceExecutionCache = new HashMap<>();

  // Shared executor service used for executing an ingestion source on a schedule
  private final ScheduledExecutorService _sharedExecutorService = Executors.newScheduledThreadPool(1);
  private final IngestionConfiguration _ingestionConfiguration;
  private final int _batchGetDelayIntervalSeconds;
  private final int _batchGetRefreshIntervalSeconds;

  public void init() {
    final BatchRefreshSchedulesRunnable batchRefreshSchedulesRunnable = new BatchRefreshSchedulesRunnable(
        _systemAuthentication,
        _entityClient,
        this::scheduleNextIngestionSourceExecution,
        this::unscheduleAll);

    // Schedule a recurring batch-reload task.
    _sharedExecutorService.scheduleAtFixedRate(
        batchRefreshSchedulesRunnable, _batchGetDelayIntervalSeconds, _batchGetRefreshIntervalSeconds,
        TimeUnit.SECONDS);
  }

  /**
   * Removes the next scheduled execution of a particular ingestion source, if it exists.
   */
  public void unscheduleNextIngestionSourceExecution(final Urn ingestionSourceUrn) {
    log.info("Unscheduling ingestion source with urn {}", ingestionSourceUrn);
    // Deleting an ingestion source schedule. Un-schedule the next execution.
    ScheduledFuture<?> future = _nextIngestionSourceExecutionCache.get(ingestionSourceUrn);
    if (future != null) {
      future.cancel(false); // Do not interrupt running processes
      _nextIngestionSourceExecutionCache.remove(ingestionSourceUrn);
    }
  }

  /**
   * Un-schedule all ingestion sources that are scheduled for execution. This is performed on refresh of ingestion sources.
   */
  public void unscheduleAll() {
    // Deleting an ingestion source schedule. Un-schedule the next execution.
    Set<Urn> scheduledSources = new HashSet<>(_nextIngestionSourceExecutionCache.keySet()); // Create copy to avoid concurrent mod.
    for (Urn urn : scheduledSources) {
      unscheduleNextIngestionSourceExecution(urn);
    }
  }

  /**
   * Computes and schedules the next execution time for a particular Ingestion Source, if it has not already been scheduled.
   */
  public void scheduleNextIngestionSourceExecution(final Urn ingestionSourceUrn, final DataHubIngestionSourceInfo newInfo) {

    // 1. Attempt to un-schedule any previous executions
    unscheduleNextIngestionSourceExecution(ingestionSourceUrn);

    if (newInfo.hasSchedule()) {

      final DataHubIngestionSourceSchedule schedule = newInfo.getSchedule();

      // 2. Schedule the next run of the ingestion source
      log.info(
          String.format("Scheduling next execution of Ingestion Source with urn %s. Schedule: %s",
              ingestionSourceUrn,
              schedule.getInterval(GetMode.NULL)));

      // Construct the new cron expression
      final String modifiedCronInterval = adjustCronInterval(schedule.getInterval());
      if (CronSequenceGenerator.isValidExpression(modifiedCronInterval)) {

        final String timezone = schedule.hasTimezone() ? schedule.getTimezone() : "UTC";
        final CronSequenceGenerator generator = new CronSequenceGenerator(modifiedCronInterval, TimeZone.getTimeZone(timezone));
        final Date currentDate = new Date();
        final Date nextExecDate = generator.next(currentDate);
        final long scheduleTime = nextExecDate.getTime() - currentDate.getTime();

        // Schedule the ingestion source to run some time in the future.
        final ExecutionRequestRunnable executionRequestRunnable = new ExecutionRequestRunnable(
            _systemAuthentication,
            _entityClient,
            _ingestionConfiguration,
            ingestionSourceUrn,
            newInfo,
            () -> _nextIngestionSourceExecutionCache.remove(ingestionSourceUrn),
            this::scheduleNextIngestionSourceExecution);

        // Schedule the next ingestion run
        final ScheduledFuture<?> scheduledFuture = _sharedExecutorService.schedule(executionRequestRunnable, scheduleTime, TimeUnit.MILLISECONDS);
        _nextIngestionSourceExecutionCache.put(ingestionSourceUrn, scheduledFuture);

        log.info(String.format("Scheduled next execution of Ingestion Source with urn %s in %sms.", ingestionSourceUrn, scheduleTime));

      } else {
        log.error(String.format("Found malformed Ingestion Source schedule: %s for urn: %s. Skipping scheduling.", schedule.getInterval(), ingestionSourceUrn));
      }

    } else {
      log.info(String.format("Ingestion source with urn %s has no configured schedule. Not scheduling.", ingestionSourceUrn));
    }
  }

  /**
   * A {@link Runnable} used to periodically re-populate the schedules cache.
   *
   * Currently, the refresh logic is not very smart. When the cache is invalidated, we simply re-fetch the
   * entire cache using schedules stored in the backend.
   */
  @VisibleForTesting
  static class BatchRefreshSchedulesRunnable implements Runnable {

    private final Authentication _systemAuthentication;
    private final EntityClient _entityClient;
    private final BiConsumer<Urn, DataHubIngestionSourceInfo> _scheduleNextIngestionSourceExecution;
    private final Runnable _unscheduleAll;

    public BatchRefreshSchedulesRunnable(
        @Nonnull final Authentication systemAuthentication,
        @Nonnull final EntityClient entityClient,
        @Nonnull final BiConsumer<Urn, DataHubIngestionSourceInfo> scheduleNextIngestionSourceExecution,
        @Nonnull final Runnable unscheduleAll) {
      _systemAuthentication = Objects.requireNonNull(systemAuthentication);
      _entityClient = Objects.requireNonNull(entityClient);
      _scheduleNextIngestionSourceExecution = Objects.requireNonNull(scheduleNextIngestionSourceExecution);
      _unscheduleAll = unscheduleAll;
    }

    @Override
    public void run() {
      try {

        int start = 0;
        int count = 30;
        int total = 30;

        while (start < total) {
          try {
            log.debug(String.format("Batch fetching ingestion source schedules. start: %s, count: %s ", start, count));

            // 1. List all ingestion source urns.
            final ListResult ingestionSourceUrns = _entityClient.list(
                Constants.INGESTION_SOURCE_ENTITY_NAME,
                Collections.emptyMap(),
                start,
                count,
                _systemAuthentication);

            // 2. Fetch all ingestion sources, specifically the "info" aspect.
            final Map<Urn, EntityResponse> ingestionSources = _entityClient.batchGetV2(
                Constants.INGESTION_SOURCE_ENTITY_NAME,
                new HashSet<>(ingestionSourceUrns.getEntities()),
                ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME),
                _systemAuthentication);

            // 3. Reschedule ingestion sources based on the fetched schedules (inside "info")
            log.debug("Received batch of Ingestion Source Info aspects. Attempting to re-schedule execution requests.");

            // First unschedule all currently scheduled runs (to make sure consistency is maintained)
            _unscheduleAll.run();

            // Then schedule the next ingestion runs
            scheduleNextIngestionRuns(new ArrayList<>(ingestionSources.values()));

            total = ingestionSourceUrns.getTotal();
            start = start + count;

          } catch (RemoteInvocationException e) {
            log.error(
                String.format("Failed to retrieve ingestion sources! Skipping updating schedule cache until next refresh. start: %s, count: %s",
                    start,
                    count),
                e);
            return;
          }
        }
        log.info(String.format("Successfully fetched %s ingestion sources.", total));
      } catch (Exception e) {
        log.error("Caught exception while loading Ingestion Sources. Will retry on next scheduled attempt.", e);
      }
    }

    /**
     * Attempts to reschedule the next ingestion source run based on a batch of {@link EntityResponse} objects
     * received from the Metadata Service.
     */
    private void scheduleNextIngestionRuns(@Nonnull final List<EntityResponse> ingestionSourceEntities) {
      for (final EntityResponse response : ingestionSourceEntities) {
        final Urn entityUrn = response.getUrn();
        final EnvelopedAspectMap aspects = response.getAspects();
        final EnvelopedAspect envelopedInfo = aspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
        final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());

        // Invoke the "scheduleNextIngestionSourceExecution" (passed from parent)
        _scheduleNextIngestionSourceExecution.accept(entityUrn, ingestionSourceInfo);
      }
    }
  }

  /**
   * A {@link Runnable} used to create Ingestion Execution Requests.
   *
   * The expectation is that there's a downstream action which is listening and executing new Execution Requests.
   */
  @VisibleForTesting
  static class ExecutionRequestRunnable implements Runnable {

    private static final String RUN_INGEST_TASK_NAME = "RUN_INGEST";
    private static final String EXECUTION_REQUEST_SOURCE_NAME = "SCHEDULED_INGESTION_SOURCE";
    private static final String RECIPE_ARGUMENT_NAME = "recipe";
    private static final String VERSION_ARGUMENT_NAME = "version";

    private final Authentication _systemAuthentication;
    private final EntityClient _entityClient;
    private final IngestionConfiguration _ingestionConfiguration;

    // Information about the ingestion source being executed
    private final Urn _ingestionSourceUrn;
    private final DataHubIngestionSourceInfo _ingestionSourceInfo;

    // Used for clearing the "next execution" cache once a corresponding execution request has been created.
    private final Runnable _deleteNextIngestionSourceExecution;

    // Used for re-scheduling the ingestion source once it has executed!
    private final BiConsumer<Urn, DataHubIngestionSourceInfo> _scheduleNextIngestionSourceExecution;

    public ExecutionRequestRunnable(
        @Nonnull final Authentication systemAuthentication,
        @Nonnull final EntityClient entityClient,
        @Nonnull final IngestionConfiguration ingestionConfiguration,
        @Nonnull final Urn ingestionSourceUrn,
        @Nonnull final DataHubIngestionSourceInfo ingestionSourceInfo,
        @Nonnull final Runnable deleteNextIngestionSourceExecution,
        @Nonnull final BiConsumer<Urn, DataHubIngestionSourceInfo> scheduleNextIngestionSourceExecution) {
      _systemAuthentication = Objects.requireNonNull(systemAuthentication);
      _entityClient = Objects.requireNonNull(entityClient);
      _ingestionConfiguration = Objects.requireNonNull(ingestionConfiguration);
      _ingestionSourceUrn = Objects.requireNonNull(ingestionSourceUrn);
      _ingestionSourceInfo = Objects.requireNonNull(ingestionSourceInfo);
      _deleteNextIngestionSourceExecution = Objects.requireNonNull(deleteNextIngestionSourceExecution);
      _scheduleNextIngestionSourceExecution = Objects.requireNonNull(scheduleNextIngestionSourceExecution);
    }

    @Override
    public void run() {

      // Remove the next ingestion execution as we are going to execute it now. (no retry logic currently)
      _deleteNextIngestionSourceExecution.run();

      try {

        log.info(String.format(
            "Creating Execution Request for scheduled Ingestion Source with urn %s",
            _ingestionSourceUrn));

        // Create a new Execution Request Proposal
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        final ExecutionRequestKey key = new ExecutionRequestKey();
        // (Give the execution request a random id)
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        key.setId(uuidStr);
        proposal.setEntityKeyAspect(GenericRecordUtils.serializeAspect(key));

        // Construct arguments (arguments) of the Execution Request
        final ExecutionRequestInput input = new ExecutionRequestInput();
        input.setTask(RUN_INGEST_TASK_NAME);
        input.setSource(new ExecutionRequestSource()
            .setType(EXECUTION_REQUEST_SOURCE_NAME)
            .setIngestionSource(_ingestionSourceUrn));
        input.setExecutorId(_ingestionSourceInfo.getConfig().getExecutorId(), SetMode.IGNORE_NULL);
        input.setRequestedAt(System.currentTimeMillis());

        Map<String, String> arguments = new HashMap<>();
        arguments.put(RECIPE_ARGUMENT_NAME, _ingestionSourceInfo.getConfig().getRecipe());
        arguments.put(VERSION_ARGUMENT_NAME, _ingestionSourceInfo.getConfig().hasVersion()
            ? _ingestionSourceInfo.getConfig().getVersion()
            : _ingestionConfiguration.getDefaultCliVersion());
        input.setArgs(new StringMap(arguments));

        proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
        proposal.setAspectName(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(input));
        proposal.setChangeType(ChangeType.UPSERT);

        _entityClient.ingestProposal(proposal, _systemAuthentication);
      } catch (Exception e) {
        // TODO: This type of thing should likely be proactively reported.
        log.error(String.format(
            "Caught exception while attempting to create Execution Request for Ingestion Source with urn %s. Will retry on next scheduled attempt.",
            _ingestionSourceUrn), e);
      }

      // 2. Re-Schedule the next execution request.
      _scheduleNextIngestionSourceExecution.accept(_ingestionSourceUrn, _ingestionSourceInfo);
    }
  }

  private String adjustCronInterval(final String origCronInterval) {
    Objects.requireNonNull(origCronInterval, "origCronInterval must not be null");
    // Typically we support 5-character cron. Spring's lib only supports 6 character cron so we make an adjustment here.
    final String[] originalCronParts = origCronInterval.split(" ");
    if (originalCronParts.length == 5) {
      return String.format("0 %s", origCronInterval);
    }
    return origCronInterval;
  }
}