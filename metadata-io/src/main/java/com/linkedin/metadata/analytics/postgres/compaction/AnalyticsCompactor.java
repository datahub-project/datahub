package com.linkedin.metadata.analytics.postgres.compaction;

import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionRequest;
import com.linkedin.metadata.analytics.compaction.AnalyticsCompactionResult;
import com.linkedin.metadata.analytics.postgres.AnalyticsMetricFamilies;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsStore;
import com.linkedin.metadata.analytics.postgres.PostgresAnalyticsUtc;
import java.sql.SQLException;
import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Progressive hour → day → month compaction with watermark sealing and per-call work budgets. Hours
 * seal when {@code now >= hour_end + input_lag}.
 */
@Slf4j
@RequiredArgsConstructor
public class AnalyticsCompactor {

  static final String IMPLEMENTATION = "pgAnalytics";

  private static final List<String> FAMILIES =
      List.of(
          AnalyticsMetricFamilies.DATAHUB_USAGE,
          AnalyticsMetricFamilies.API_USAGE,
          AnalyticsMetricFamilies.SYSTEM_USAGE);

  @Nonnull private final PgAnalyticsStoreRegistry registry;

  @Nonnull
  public AnalyticsCompactionResult compact(@Nonnull AnalyticsCompactionRequest request) {
    Instant started = Instant.now();
    Instant deadline = started.plusMillis(Math.max(1L, request.getMaxWallClockMillis()));
    int hoursSealed = 0;
    int daysCompacted = 0;
    int monthsCompacted = 0;
    boolean moreWork = false;
    boolean failed = false;
    String failureMessage = null;

    for (PgAnalyticsStoreRegistry.StoreHandle handle : registry.allStores().values()) {
      try {
        StoreCompactOutcome outcome =
            compactStore(
                handle.getStore(),
                handle.getOptions().getInputLagSeconds(),
                Instant.now(),
                request,
                deadline);
        hoursSealed += outcome.hoursSealed;
        daysCompacted += outcome.daysCompacted;
        monthsCompacted += outcome.monthsCompacted;
        moreWork = moreWork || outcome.moreWorkRemaining;
      } catch (SQLException e) {
        log.warn("pgAnalytics compaction failed for store {}", handle.getOptions().getName(), e);
        moreWork = true;
        failed = true;
        if (failureMessage == null) {
          failureMessage =
              "Compaction failed for store "
                  + handle.getOptions().getName()
                  + ": "
                  + e.getMessage();
        }
      }
    }

    long duration = Instant.now().toEpochMilli() - started.toEpochMilli();
    log.info(
        "pgAnalytics compact hoursSealed={} daysCompacted={} monthsCompacted={} moreWork={} failed={} durationMs={}",
        hoursSealed,
        daysCompacted,
        monthsCompacted,
        moreWork,
        failed,
        duration);
    return AnalyticsCompactionResult.builder()
        .lockNotAcquired(false)
        .moreWorkRemaining(moreWork)
        .failed(failed)
        .hoursSealed(hoursSealed)
        .daysCompacted(daysCompacted)
        .monthsCompacted(monthsCompacted)
        .durationMillis(duration)
        .implementation(IMPLEMENTATION)
        .message(failureMessage)
        .build();
  }

  @Nonnull
  StoreCompactOutcome compactStore(
      @Nonnull PostgresAnalyticsStore store,
      int inputLagSeconds,
      @Nonnull Instant now,
      @Nonnull AnalyticsCompactionRequest request,
      @Nonnull Instant deadline)
      throws SQLException {
    int hoursSealed = 0;
    boolean moreWork = false;
    int hourLookbackHours = Math.max(1, request.getHourLookbackHours());
    int dayLookbackDays = Math.max(1, request.getDayLookbackDays());
    int monthLookbackMonths = Math.max(1, request.getMonthLookbackMonths());

    Instant lookbackStart =
        PostgresAnalyticsUtc.truncateToUtcHour(now).minus(hourLookbackHours, ChronoUnit.HOURS);
    Instant openHour = PostgresAnalyticsUtc.truncateToUtcHour(now);
    Instant cursor = hourSealCursorStart(store, lookbackStart);
    while (cursor.isBefore(openHour)) {
      if (Instant.now().isAfter(deadline)) {
        moreWork = true;
        break;
      }
      if (PostgresAnalyticsUtc.isHourSealable(cursor, now, inputLagSeconds)
          && !isHourSealed(store, cursor)) {
        if (hoursSealed >= request.getMaxHoursToSeal()) {
          moreWork = true;
          break;
        }
        sealHour(store, cursor);
        hoursSealed++;
      }
      cursor = cursor.plus(1, ChronoUnit.HOURS);
    }
    if (!moreWork) {
      moreWork = hasMoreSealableHours(store, cursor, openHour, now, inputLagSeconds);
    }

    int daysCompacted = 0;
    Instant dayCursor =
        PostgresAnalyticsUtc.truncateToUtcDay(now).minus(dayLookbackDays, ChronoUnit.DAYS);
    Instant openDay = PostgresAnalyticsUtc.truncateToUtcDay(now);
    while (dayCursor.isBefore(openDay)) {
      if (Instant.now().isAfter(deadline)) {
        moreWork = true;
        break;
      }
      boolean needWork = false;
      boolean didWork = false;
      for (String family : FAMILIES) {
        if (!isDaySealed(store, family, dayCursor) && store.isDayFullySealed(family, dayCursor)) {
          needWork = true;
          if (daysCompacted >= request.getMaxDaysToCompact()) {
            moreWork = true;
            break;
          }
          store.compactHoursToDay(family, dayCursor);
          store.upsertWatermark(
              AnalyticsMetricFamilies.LAYER_DAY,
              family,
              PostgresAnalyticsUtc.partitionKeyDay(dayCursor),
              dayCursor.plus(1, ChronoUnit.DAYS));
          didWork = true;
        }
      }
      if (moreWork && needWork && !didWork) {
        break;
      }
      if (didWork) {
        daysCompacted++;
      }
      dayCursor = dayCursor.plus(1, ChronoUnit.DAYS);
    }

    int monthsCompacted = 0;
    YearMonth openMonth = YearMonth.from(now.atZone(ZoneOffset.UTC));
    YearMonth monthCursor = openMonth.minusMonths(monthLookbackMonths);
    while (monthCursor.isBefore(openMonth)) {
      if (Instant.now().isAfter(deadline)) {
        moreWork = true;
        break;
      }
      Instant monthStart = monthCursor.atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant();
      boolean needWork = false;
      boolean didWork = false;
      for (String family : FAMILIES) {
        if (!isMonthSealed(store, family, monthStart)
            && isMonthFullySealed(store, family, monthCursor)) {
          needWork = true;
          if (monthsCompacted >= request.getMaxMonthsToCompact()) {
            moreWork = true;
            break;
          }
          store.compactDaysToMonth(family, monthStart);
          store.upsertWatermark(
              AnalyticsMetricFamilies.LAYER_MONTH,
              family,
              PostgresAnalyticsUtc.partitionKeyMonth(monthStart),
              monthCursor.plusMonths(1).atDay(1).atStartOfDay(ZoneOffset.UTC).toInstant());
          didWork = true;
        }
      }
      if (moreWork && needWork && !didWork) {
        break;
      }
      if (didWork) {
        monthsCompacted++;
      }
      monthCursor = monthCursor.plusMonths(1);
    }

    return new StoreCompactOutcome(hoursSealed, daysCompacted, monthsCompacted, moreWork);
  }

  private static boolean isHourSealed(PostgresAnalyticsStore store, Instant hourStart)
      throws SQLException {
    String partitionKey = PostgresAnalyticsUtc.partitionKeyHour(hourStart);
    for (String family : FAMILIES) {
      if (store.getSealedThrough(AnalyticsMetricFamilies.LAYER_HOUR, family, partitionKey)
          == null) {
        return false;
      }
    }
    return true;
  }

  /**
   * Continue from the watermark frontier so outages beyond the default lookback still catch up.
   * Cold-start (any family missing hour watermarks) uses {@code lookbackStart}.
   */
  private static Instant hourSealCursorStart(
      @Nonnull PostgresAnalyticsStore store, @Nonnull Instant lookbackStart) throws SQLException {
    Instant frontier = null;
    for (String family : FAMILIES) {
      Instant latest = store.getLatestSealedHourStart(family);
      if (latest == null) {
        return lookbackStart;
      }
      frontier = frontier == null || latest.isBefore(frontier) ? latest : frontier;
    }
    return frontier.plus(1, ChronoUnit.HOURS);
  }

  private static boolean isDaySealed(PostgresAnalyticsStore store, String family, Instant dayStart)
      throws SQLException {
    return store.getSealedThrough(
            AnalyticsMetricFamilies.LAYER_DAY,
            family,
            PostgresAnalyticsUtc.partitionKeyDay(dayStart))
        != null;
  }

  private static boolean isMonthSealed(
      PostgresAnalyticsStore store, String family, Instant monthStart) throws SQLException {
    return store.getSealedThrough(
            AnalyticsMetricFamilies.LAYER_MONTH,
            family,
            PostgresAnalyticsUtc.partitionKeyMonth(monthStart))
        != null;
  }

  private static boolean hasMoreSealableHours(
      PostgresAnalyticsStore store,
      Instant from,
      Instant openHour,
      Instant now,
      int inputLagSeconds)
      throws SQLException {
    Instant cursor = from;
    while (cursor.isBefore(openHour)) {
      if (PostgresAnalyticsUtc.isHourSealable(cursor, now, inputLagSeconds)
          && !isHourSealed(store, cursor)) {
        return true;
      }
      cursor = cursor.plus(1, ChronoUnit.HOURS);
    }
    return false;
  }

  private void sealHour(@Nonnull PostgresAnalyticsStore store, @Nonnull Instant hourStart)
      throws SQLException {
    String partitionKey = PostgresAnalyticsUtc.partitionKeyHour(hourStart);
    Instant sealedThrough = PostgresAnalyticsUtc.hourEndExclusive(hourStart);
    if (store.getSealedThrough(
            AnalyticsMetricFamilies.LAYER_HOUR, AnalyticsMetricFamilies.DATAHUB_USAGE, partitionKey)
        == null) {
      store.materializeDatahubUsageHourlyFromRaw(hourStart);
    }
    for (String family : FAMILIES) {
      if (store.getSealedThrough(AnalyticsMetricFamilies.LAYER_HOUR, family, partitionKey)
          == null) {
        store.upsertWatermark(
            AnalyticsMetricFamilies.LAYER_HOUR, family, partitionKey, sealedThrough);
      }
    }
  }

  private static boolean isMonthFullySealed(
      PostgresAnalyticsStore store, String family, YearMonth month) throws SQLException {
    for (int day = 1; day <= month.lengthOfMonth(); day++) {
      Instant dayStart = month.atDay(day).atStartOfDay(ZoneOffset.UTC).toInstant();
      if (!isDaySealed(store, family, dayStart)) {
        return false;
      }
    }
    return true;
  }

  record StoreCompactOutcome(
      int hoursSealed, int daysCompacted, int monthsCompacted, boolean moreWorkRemaining) {}
}
