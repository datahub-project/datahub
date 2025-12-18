package com.linkedin.gms.factory.statistics;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.dataset.DatasetFieldProfile;
import com.linkedin.dataset.DatasetFieldProfileArray;
import com.linkedin.dataset.DatasetFieldUsageCounts;
import com.linkedin.dataset.DatasetFieldUsageCountsArray;
import com.linkedin.dataset.DatasetProfile;
import com.linkedin.dataset.DatasetUsageStatistics;
import com.linkedin.dataset.DatasetUserUsageCounts;
import com.linkedin.dataset.DatasetUserUsageCountsArray;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.PartitionSpec;
import com.linkedin.timeseries.PartitionType;
import com.linkedin.timeseries.TimeWindowSize;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Random;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Service to generate realistic statistics for the order_details dataset.
 *
 * <p>Generates both datasetProfile (row counts, column counts, field profiles) and
 * datasetUsageStatistics (query counts, user counts, field usage).
 */
@Slf4j
@RequiredArgsConstructor
public class OrderDetailsStatisticsGenerator {

  public static final String ORDER_DETAILS_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:snowflake,order_entry_db.analytics.order_details,PROD)";

  private static final int BASE_ROW_COUNT = 100000;
  private static final int ROW_COUNT_INCREMENT_MIN = 1000;
  private static final int ROW_COUNT_INCREMENT_MAX = 5000;
  private static final int COLUMN_COUNT = 55;
  private static final long BYTES_PER_ROW = 500; // Estimated average row size

  private final EntityService<?> _entityService;
  private final TimeseriesAspectService _timeseriesAspectService;

  /**
   * Check if statistics already exist for the dataset.
   *
   * @param context The operation context
   * @return true if statistics exist, false otherwise
   */
  public boolean statisticsExist(@Nonnull OperationContext context) {
    try {
      final Urn datasetUrn = Urn.createFromString(ORDER_DETAILS_DATASET_URN);
      // Check if any datasetProfile exists
      final List<EnvelopedAspect> existingProfiles =
          _timeseriesAspectService.getAspectValues(
              context,
              datasetUrn,
              Constants.DATASET_ENTITY_NAME,
              Constants.DATASET_PROFILE_ASPECT_NAME,
              null,
              null,
              1,
              null);
      return existingProfiles != null && !existingProfiles.isEmpty();
    } catch (Exception e) {
      log.warn("Failed to check if statistics exist, assuming they don't: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Check if the order_details dataset exists in the database.
   *
   * @param context The operation context
   * @return true if the dataset exists, false otherwise
   */
  public boolean datasetExists(@Nonnull OperationContext context) {
    try {
      final Urn datasetUrn = Urn.createFromString(ORDER_DETAILS_DATASET_URN);
      return _entityService.exists(context, datasetUrn, true);
    } catch (Exception e) {
      log.warn("Failed to check if order_details dataset exists: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Generate and ingest statistics for the order_details dataset.
   *
   * @param context The operation context
   * @param timestampMillis The timestamp for the statistics (start of day in milliseconds)
   */
  public void generateAndIngestStatistics(@Nonnull OperationContext context, long timestampMillis) {
    try {
      final Urn datasetUrn = Urn.createFromString(ORDER_DETAILS_DATASET_URN);

      log.info("Generating statistics for order_details dataset at timestamp: {}", timestampMillis);

      // Generate datasetProfile
      final DatasetProfile datasetProfile = generateDatasetProfile(timestampMillis);
      ingestAspect(context, datasetUrn, Constants.DATASET_PROFILE_ASPECT_NAME, datasetProfile);

      // Generate datasetUsageStatistics
      final DatasetUsageStatistics usageStatistics =
          generateDatasetUsageStatistics(timestampMillis);
      ingestAspect(
          context, datasetUrn, Constants.DATASET_USAGE_STATISTICS_ASPECT_NAME, usageStatistics);

      log.info("Successfully generated and ingested statistics for order_details dataset");
    } catch (Exception e) {
      log.error("Failed to generate statistics for order_details dataset", e);
      throw new RuntimeException("Failed to generate statistics", e);
    }
  }

  /**
   * Generate and ingest statistics for multiple days (for initial historical data).
   *
   * @param context The operation context
   * @param startTimestampMillis The start timestamp (inclusive)
   * @param days The number of days to generate
   */
  public void generateHistoricalStatistics(
      @Nonnull OperationContext context, long startTimestampMillis, int days) {
    log.info(
        "Generating {} days of historical statistics starting from {}", days, startTimestampMillis);

    final DateTime startDate = new DateTime(startTimestampMillis, DateTimeZone.UTC);
    for (int i = 0; i < days; i++) {
      final DateTime dayDate = startDate.plusDays(i);
      final long dayTimestamp = dayDate.withTimeAtStartOfDay().getMillis();
      generateAndIngestStatistics(context, dayTimestamp);
    }

    log.info("Successfully generated {} days of historical statistics", days);
  }

  /**
   * Generate a DatasetProfile with realistic values.
   *
   * @param timestampMillis The timestamp for the profile
   * @return A DatasetProfile object
   */
  private DatasetProfile generateDatasetProfile(long timestampMillis) {
    // Calculate row count: base + monotonic increment based on days since a reference date
    // Uses day-seeded random for consistent per-day variation while ensuring monotonic growth
    final DateTime referenceDate = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeZone.UTC);
    final DateTime currentDate = new DateTime(timestampMillis, DateTimeZone.UTC);
    final int daysSinceReference =
        (int) ((currentDate.getMillis() - referenceDate.getMillis()) / (24 * 60 * 60 * 1000));

    // Seed random with day to get consistent value for each specific day
    final long daysSeed = timestampMillis / (24 * 60 * 60 * 1000);
    final Random dayRandom = new Random(daysSeed);

    // Base monotonic growth (average daily increment) + small per-day variation
    // Ensure rowCount is always positive to avoid division by zero in proportion calculations
    final int avgDailyIncrement = (ROW_COUNT_INCREMENT_MIN + ROW_COUNT_INCREMENT_MAX) / 2;
    final int dailyVariation = dayRandom.nextInt(1000) - 500; // -500 to +500
    final long rowCount =
        Math.max(
            1, BASE_ROW_COUNT + ((long) daysSinceReference * avgDailyIncrement) + dailyVariation);

    final DatasetProfile profile = new DatasetProfile();
    profile.setTimestampMillis(timestampMillis);
    profile.setEventGranularity(new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1));
    // Set partitionSpec so the GraphQL query can find this profile
    profile.setPartitionSpec(
        new PartitionSpec().setPartition("FULL_TABLE_SNAPSHOT").setType(PartitionType.FULL_TABLE));
    profile.setRowCount(rowCount);
    profile.setColumnCount((long) COLUMN_COUNT);
    profile.setSizeInBytes(rowCount * BYTES_PER_ROW);

    // Generate field profiles for key fields
    final DatasetFieldProfileArray fieldProfiles = new DatasetFieldProfileArray();
    fieldProfiles.add(createFieldProfile("order_id", rowCount, 0, rowCount));
    fieldProfiles.add(
        createFieldProfile(
            "customer_id", rowCount, 0, (long) (rowCount * 0.3))); // ~30% unique customers
    fieldProfiles.add(
        createFieldProfile(
            "order_total", rowCount, 0, null, "10.00", "9999.99", "125.50", "99.99", "45.23"));
    fieldProfiles.add(createFieldProfile("order_date", rowCount, 0, null));
    fieldProfiles.add(
        createFieldProfile(
            "product_id", rowCount, 0, (long) (rowCount * 0.1))); // ~10% unique products
    fieldProfiles.add(
        createFieldProfile("quantity", rowCount, 0, null, "1", "100", "5.5", "4", "2.1"));
    fieldProfiles.add(
        createFieldProfile(
            "unit_price", rowCount, 0, null, "1.00", "500.00", "25.75", "20.00", "10.50"));

    profile.setFieldProfiles(fieldProfiles);

    return profile;
  }

  /** Create a field profile for a specific field. */
  private DatasetFieldProfile createFieldProfile(
      String fieldPath, long rowCount, long nullCount, Long uniqueCount) {
    return createFieldProfile(
        fieldPath, rowCount, nullCount, uniqueCount, null, null, null, null, null);
  }

  /** Create a field profile with min/max/mean/median/stdev. */
  private DatasetFieldProfile createFieldProfile(
      String fieldPath,
      long rowCount,
      long nullCount,
      Long uniqueCount,
      String min,
      String max,
      String mean,
      String median,
      String stdev) {
    final DatasetFieldProfile fieldProfile = new DatasetFieldProfile();
    fieldProfile.setFieldPath(fieldPath);
    fieldProfile.setNullCount(nullCount);
    fieldProfile.setNullProportion((float) ((double) nullCount / rowCount));
    if (uniqueCount != null) {
      fieldProfile.setUniqueCount(uniqueCount);
      fieldProfile.setUniqueProportion((float) ((double) uniqueCount / rowCount));
    }
    if (min != null) {
      fieldProfile.setMin(min);
    }
    if (max != null) {
      fieldProfile.setMax(max);
    }
    if (mean != null) {
      fieldProfile.setMean(mean);
    }
    if (median != null) {
      fieldProfile.setMedian(median);
    }
    if (stdev != null) {
      fieldProfile.setStdev(stdev);
    }
    return fieldProfile;
  }

  /**
   * Generate DatasetUsageStatistics with realistic values.
   *
   * @param timestampMillis The timestamp for the statistics
   * @return A DatasetUsageStatistics object
   */
  private DatasetUsageStatistics generateDatasetUsageStatistics(long timestampMillis) {
    final DatasetUsageStatistics usageStatistics = new DatasetUsageStatistics();
    usageStatistics.setTimestampMillis(timestampMillis);
    usageStatistics.setEventGranularity(
        new TimeWindowSize().setUnit(CalendarInterval.DAY).setMultiple(1));

    // Seed random with day to get consistent value for each specific day (idempotent generation)
    final long daysSeed = timestampMillis / (24 * 60 * 60 * 1000);
    final Random dayRandom = new Random(daysSeed);

    // Generate random but realistic values
    final int uniqueUserCount = 5 + dayRandom.nextInt(16); // 5-20 users
    final int totalSqlQueries = 50 + dayRandom.nextInt(151); // 50-200 queries

    usageStatistics.setUniqueUserCount(uniqueUserCount);
    usageStatistics.setTotalSqlQueries(totalSqlQueries);

    // Generate top SQL queries
    final StringArray topSqlQueries = new StringArray();
    topSqlQueries.add(
        "SELECT * FROM order_entry_db.analytics.order_details WHERE order_date >= CURRENT_DATE - 7");
    topSqlQueries.add(
        "SELECT customer_id, SUM(order_total) FROM order_entry_db.analytics.order_details GROUP BY customer_id");
    topSqlQueries.add(
        "SELECT product_id, COUNT(*) FROM order_entry_db.analytics.order_details WHERE order_date = CURRENT_DATE GROUP BY product_id");
    topSqlQueries.add(
        "SELECT * FROM order_entry_db.analytics.order_details WHERE order_status = 'COMPLETED' LIMIT 100");
    topSqlQueries.add(
        "SELECT order_id, order_total, customer_id FROM order_entry_db.analytics.order_details ORDER BY order_date DESC LIMIT 50");
    usageStatistics.setTopSqlQueries(topSqlQueries);

    // Generate user counts (distribute queries across users)
    final DatasetUserUsageCountsArray userCounts = new DatasetUserUsageCountsArray();
    int remainingQueries = totalSqlQueries;
    for (int i = 0; i < uniqueUserCount && remainingQueries > 0; i++) {
      final DatasetUserUsageCounts userCount = new DatasetUserUsageCounts();
      // Use example.com users from the sample data
      try {
        userCount.setUser(
            Urn.createFromString(String.format("urn:li:corpuser:user%d@example.com", i + 1)));
      } catch (Exception e) {
        log.error("Failed to create user URN", e);
        continue;
      }
      final int userQueryCount =
          (i == uniqueUserCount - 1)
              ? remainingQueries
              : Math.min(remainingQueries, 5 + dayRandom.nextInt(Math.min(remainingQueries, 30)));
      userCount.setCount(userQueryCount);
      userCounts.add(userCount);
      remainingQueries -= userQueryCount;
    }
    usageStatistics.setUserCounts(userCounts);

    // Generate field counts (most frequently accessed fields)
    final DatasetFieldUsageCountsArray fieldCounts = new DatasetFieldUsageCountsArray();
    final String[] topFields = {
      "order_id", "customer_id", "order_total", "order_date", "product_id", "quantity", "unit_price"
    };
    int remainingFieldQueries = totalSqlQueries;
    for (int i = 0; i < topFields.length && remainingFieldQueries > 0; i++) {
      final DatasetFieldUsageCounts fieldCount = new DatasetFieldUsageCounts();
      fieldCount.setFieldPath(topFields[i]);
      // Most common fields get more queries
      final int fieldQueryCount =
          (i == topFields.length - 1)
              ? remainingFieldQueries
              : (totalSqlQueries / (topFields.length - i) + dayRandom.nextInt(10));
      fieldCount.setCount(Math.min(fieldQueryCount, remainingFieldQueries));
      fieldCounts.add(fieldCount);
      remainingFieldQueries -= fieldCount.getCount();
    }
    usageStatistics.setFieldCounts(fieldCounts);

    return usageStatistics;
  }

  /** Ingest an aspect using EntityService. */
  private void ingestAspect(
      @Nonnull OperationContext context,
      @Nonnull Urn entityUrn,
      @Nonnull String aspectName,
      @Nonnull com.linkedin.data.template.RecordTemplate aspect) {
    try {
      final AuditStamp auditStamp =
          new AuditStamp()
              .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
              .setTime(System.currentTimeMillis());

      final MetadataChangeProposal proposal =
          AspectUtils.buildMetadataChangeProposal(entityUrn, aspectName, aspect);

      _entityService.ingestProposal(context, proposal, auditStamp, false);

      log.debug("Successfully ingested aspect {} for entity {}", aspectName, entityUrn);
    } catch (Exception e) {
      log.error(
          "Failed to ingest aspect {} for entity {}: {}", aspectName, entityUrn, e.getMessage(), e);
      throw new RuntimeException("Failed to ingest aspect", e);
    }
  }
}
