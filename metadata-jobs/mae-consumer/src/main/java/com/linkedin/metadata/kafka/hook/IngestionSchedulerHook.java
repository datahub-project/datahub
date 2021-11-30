package com.linkedin.metadata.kafka.hook;

import com.datahub.authentication.Authentication;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringMap;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestInput;
import com.linkedin.execution.ExecutionRequestSource;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.entity.RestliEntityClientFactory;
import com.linkedin.gms.factory.entityregistry.EntityRegistryFactory;
import com.linkedin.ingestion.DataHubIngestionSourceInfo;
import com.linkedin.ingestion.DataHubIngestionSourceSchedule;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericAspectUtils;
import com.linkedin.mxe.MetadataChangeLog;
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
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.scheduling.support.CronSequenceGenerator;
import org.springframework.stereotype.Component;


@Slf4j
@Component
@Import({EntityRegistryFactory.class, SystemAuthenticationFactory.class, RestliEntityClientFactory.class})
public class IngestionSchedulerHook implements MetadataChangeLogHook {

  private static final int DEFAULT_DELAY_INTERVAL_SECONDS = 30; // Wait 30 seconds before batch loading the schedules.
  private static final int DEFAULT_REFRESH_INTERVAL_SECONDS = 86400; // Refresh the list of ingestion sources once per day.

  private final EntityRegistry _entityRegistry;
  private final Authentication _systemAuthentication;
  private final EntityClient _entityClient;
  private final Map<String, ScheduledFuture> _nextExecutionCache = new HashMap<>();
  private final ScheduledExecutorService _sharedExecutorService = Executors.newScheduledThreadPool(1);
  private final ScheduleRefreshRunnable _scheduleRefreshRunnable;

  @Autowired
  public IngestionSchedulerHook(
      final EntityRegistry entityRegistry,
      final Authentication systemAuthentication,
      final RestliEntityClient entityClient
  ) {
    _entityRegistry = entityRegistry;
    _systemAuthentication = systemAuthentication;
    _entityClient = entityClient;
    _scheduleRefreshRunnable = new ScheduleRefreshRunnable(
        systemAuthentication,
        entityClient,
        this::scheduleNextExecution);
    _sharedExecutorService.scheduleAtFixedRate(
        _scheduleRefreshRunnable,
        DEFAULT_DELAY_INTERVAL_SECONDS,
        DEFAULT_REFRESH_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  @Override
  public void invoke(MetadataChangeLog event) {
    EntitySpec entitySpec;
    try {
      entitySpec = _entityRegistry.getEntitySpec(event.getEntityType());
    } catch (IllegalArgumentException e) {
      log.error("Error while processing entity type {}: {}", event.getEntityType(), e.toString());
      return;
    }
    Urn urn = EntityKeyUtils.getUrnFromLog(event, entitySpec.getKeyAspectSpec());

    if (Constants.INGESTION_INFO_ASPECT_NAME.equals(event.getAspectName())) {
      // The ingestion info aspect ic changing.
      if (ChangeType.UPSERT.equals(event.getChangeType()) || ChangeType.CREATE.equals(event.getChangeType())) {

        log.info(String.format("Received %s to Ingestion Source. Rescheduling the source (if applicable). urn: %s, key: %s.",
            event.getChangeType(),
            event.getEntityUrn(),
            event.getEntityKeyAspect()));

        // Update the cache to contain the new thing.
        final DataHubIngestionSourceInfo info = (DataHubIngestionSourceInfo) GenericAspectUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            entitySpec.getAspectSpec(Constants.INGESTION_INFO_ASPECT_NAME));

        // Schedule the next execution of the source.
        scheduleNextExecution(urn, info);

      } else if (ChangeType.DELETE.equals(event.getChangeType())) {
        // Unschedule the task.
        ScheduledFuture<?> future = _nextExecutionCache.get(urn.toString());
        if (future != null) {
          future.cancel(true);
        }
      }
    }
  }

  private void scheduleNextExecution(final Urn entityUrn, final DataHubIngestionSourceInfo info) {
    if (info.hasSchedule()) {

      final DataHubIngestionSourceSchedule schedule = info.getSchedule();

      // Check whether there is already a job scheduled for it.
      if (!_nextExecutionCache.containsKey(entityUrn.toString())) {
        log.info(String.format("Scheduling next execution of Ingestion Source with urn %s. Schedule: %s", entityUrn, schedule.getInterval()));

        final String modifiedCronInterval = adjustCronInterval(schedule.getInterval());
        if (CronSequenceGenerator.isValidExpression(modifiedCronInterval)) {
          CronSequenceGenerator generator = new CronSequenceGenerator(modifiedCronInterval, TimeZone.getTimeZone("UTC"));
          Date currentDate = new Date();

          Date nextExecDate = generator.next(currentDate);
          long scheduleTime = nextExecDate.getTime() - currentDate.getTime();

          // Create the runnable
          final ExecutionRequestRunnable runnable = new ExecutionRequestRunnable(
              _systemAuthentication,
              _entityClient,
              entityUrn,
              info,
              () ->_nextExecutionCache.remove(entityUrn.toString()),
              this::scheduleNextExecution);

          // Schedule the process.
          ScheduledFuture<?> scheduledFuture = _sharedExecutorService.schedule(runnable, scheduleTime, TimeUnit.MILLISECONDS);
          _nextExecutionCache.put(entityUrn.toString(), scheduledFuture);
          log.info(String.format("Scheduled next execution of Ingestion Source with urn %s in %sms.", entityUrn, scheduleTime));
        } else {
          log.warn(String.format("Found malformed Ingestion Source schedule: %s for urn: %s. Skipping scheduling.", schedule.getInterval(), entityUrn));
        }
      } else {
        log.info(String.format("Ingestion source with urn %s is already scheduled. Skipping scheduling next execution.", entityUrn));
      }
    } else {
      log.info(String.format("Ingestion source with urn %s has no configured schedule. Skipping scheduling next execution.", entityUrn));
    }
  }

  private

  /**
   * A {@link Runnable} used to periodically fetch a new instance of the schedules cache.
   *
   * Currently, the refresh logic is not very smart. When the cache is invalidated, we simply re-fetch the
   * entire cache using schedules stored in the backend.
   */
  @VisibleForTesting
  static class ScheduleRefreshRunnable implements Runnable {

    private static final String INGESTION_SOURCE_ENTITY_NAME = "dataHubIngestionSource";

    private final Authentication _systemAuthentication;
    private final EntityClient _entityClient;
    private final BiConsumer<Urn, DataHubIngestionSourceInfo> _scheduleNextExecution;

    public ScheduleRefreshRunnable(
        final Authentication systemAuthentication,
        final EntityClient entityClient,
        final BiConsumer<Urn, DataHubIngestionSourceInfo> scheduleNextExecution) {
      _systemAuthentication = systemAuthentication;
      _entityClient = entityClient;
      _scheduleNextExecution = scheduleNextExecution;
    }

    @Override
    public void run() {
      try {

        int start = 0;
        int count = 30;
        int total = 30;

        while (start < total) {
          try {
            log.debug(String.format("Batch fetching schedules. start: %s, count: %s ", start, count));
            final ListResult ingestionSourceUrns = _entityClient.list(
                INGESTION_SOURCE_ENTITY_NAME,
                Collections.emptyMap(),
                start,
                count,
                _systemAuthentication);

            final Map<Urn, EntityResponse> ingestionSources = _entityClient.batchGetV2(
                Constants.INGESTION_SOURCE_ENTITY_NAME,
                new HashSet<>(ingestionSourceUrns.getEntities()),
                ImmutableSet.of(Constants.INGESTION_INFO_ASPECT_NAME),
                _systemAuthentication);

            log.info("Received batch of Ingestion Sources. Attempting to re-schedule execution requests.");
            scheduleNextIngestionRuns(new ArrayList<>(ingestionSources.values()));

            total = ingestionSourceUrns.getTotal();
            start = start + count;

          } catch (RemoteInvocationException e) {
            log.error(String.format("Failed to retrieve ingestion source urns! Skipping updating schedule cache until next refresh. start: %s, count: %s", start, count), e);
            return;
          }
        }
        log.info(String.format("Successfully fetched %s ingestion sources.", total));
      } catch (Exception e) {
        log.error("Caught exception while loading Ingestion Sources. Will retry on next scheduled attempt.", e);
      }
    }

    private void scheduleNextIngestionRuns(final List<EntityResponse> entities) {
      for (final EntityResponse response : entities) {
        final Urn entityUrn = response.getUrn();
        final EnvelopedAspectMap aspects = response.getAspects();
        final EnvelopedAspect envelopedInfo = aspects.get(Constants.INGESTION_INFO_ASPECT_NAME);
        final DataHubIngestionSourceInfo ingestionSourceInfo = new DataHubIngestionSourceInfo(envelopedInfo.getValue().data());
        _scheduleNextExecution.accept(entityUrn, ingestionSourceInfo);
      }
    }
  }

  /**
   * A {@link Runnable} used to create Ingestion Execution Requests.
   */
  @VisibleForTesting
  static class ExecutionRequestRunnable implements Runnable {

    private final Authentication _systemAuthentication;
    private final EntityClient _entityClient;
    private final Urn _urn;
    private final DataHubIngestionSourceInfo _info;
    private final Runnable _clearNextExecution;
    private final BiConsumer<Urn, DataHubIngestionSourceInfo> _scheduleNextExecution;

    public ExecutionRequestRunnable(
        final Authentication systemAuthentication,
        final EntityClient entityClient,
        final Urn urn,
        final DataHubIngestionSourceInfo info,
        final Runnable clearNextExecution,
        final BiConsumer<Urn, DataHubIngestionSourceInfo> scheduleNextExecution) {
      _systemAuthentication = systemAuthentication;
      _entityClient = entityClient;
      _urn = urn;
      _info = info;
      _clearNextExecution = clearNextExecution;
      _scheduleNextExecution = scheduleNextExecution;
    }

    @Override
    public void run() {
      // Pop the next execution run off the set of scheduled runs.
      _clearNextExecution.run();

      try {

        log.info(String.format("Triggered scheduled Execution Request for scheduled Ingestion Source with urn %s", _urn));

        // 1. Create an execution request.
        final MetadataChangeProposal proposal = new MetadataChangeProposal();

        // Create the Ingestion source key --> use the display name as a unique id to ensure it's not duplicated.
        final ExecutionRequestKey key = new ExecutionRequestKey();
        final UUID uuid = UUID.randomUUID();
        final String uuidStr = uuid.toString();
        key.setId(uuidStr);
        proposal.setEntityKeyAspect(GenericAspectUtils.serializeAspect(key));

        // Build the arguments map.
        final ExecutionRequestInput execInput = new ExecutionRequestInput();
        execInput.setTask("RUN_INGEST"); // Set the RUN_INGEST task
        execInput.setSource(new ExecutionRequestSource().setType("INGESTION_SOURCE").setIngestionSource(_urn));
        Map<String, String> arguments = ImmutableMap.of("recipe", _info.getConfig().getRecipe().getJson());
        execInput.setArgs(new StringMap(arguments));

        proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
        proposal.setAspectName(Constants.EXECUTION_REQUEST_INPUT_ASPECT_NAME);
        proposal.setAspect(GenericAspectUtils.serializeAspect(execInput));
        proposal.setChangeType(ChangeType.UPSERT);

        _entityClient.ingestProposal(proposal, _systemAuthentication);

      } catch (Exception e) {
        log.error(String.format(
            "Caught exception while attempting to create Execution Request for Ingestion Source with urn %s. Will retry on next scheduled attempt.", _urn), e);
        // TODO: Consider reslotting execution for another run.
        // TODO: Consider storing the reason for failure somewhere.
      }

      // 2. Re-Schedule the next execution request.
      _scheduleNextExecution.accept(_urn, _info);
    }
  }

  private String adjustCronInterval(final String origCronInterval) {
    Objects.requireNonNull(origCronInterval, "origCronInterval must not be null");

    final String[] originalCronParts = origCronInterval.split(" ");

    // Typically we support 5-character cron. Spring's lib only supports 6 character cron so we make an adjustment here.
    if (originalCronParts.length == 5) {
      return String.format("0 %s", origCronInterval);
    }
    return origCronInterval;
  }
}
