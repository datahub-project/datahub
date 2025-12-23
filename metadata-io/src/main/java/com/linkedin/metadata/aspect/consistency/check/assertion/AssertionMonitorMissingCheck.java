package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.*;

import com.google.common.collect.ImmutableSet;
import com.linkedin.assertion.AssertionInfo;
import com.linkedin.assertion.AssertionSourceType;
import com.linkedin.assertion.AssertionType;
import com.linkedin.common.CronSchedule;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.MonitorUrnUtils;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Check that native assertions have an associated monitor.
 *
 * <p>Native assertions (FRESHNESS, VOLUME, SQL, FIELD, DATA_SCHEMA) require a monitor for execution
 * by DataHub. External assertions (from dbt, Great Expectations, etc.) do NOT require monitors.
 *
 * <p>External assertions are identified by:
 *
 * <ul>
 *   <li>AssertionType.DATASET - Legacy, non-native assertion type
 *   <li>AssertionType.CUSTOM - Custom assertions from external tools
 *   <li>AssertionSourceType.EXTERNAL - Explicitly marked as external
 *   <li>No source defined - Implies external/legacy
 * </ul>
 *
 * <p>Note: This check creates a disabled monitor for the assertion. The monitor will need to be
 * enabled and configured before the assertion can be executed.
 *
 * <p><b>Configuration:</b> The cron schedule for newly created monitors is configured in
 * application.yaml under:
 *
 * <pre>
 * consistencyChecks:
 *   checks:
 *     assertion-monitor-missing:
 *       defaultCronSchedule: "0 0 0 * * ?"
 *       defaultCronTimezone: "UTC"
 * </pre>
 */
@Slf4j
@Component("consistencyAssertionMonitorMissingCheck")
public class AssertionMonitorMissingCheck extends AbstractAssertionCheck {

  public AssertionMonitorMissingCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  /** Configuration key for default cron schedule (required) */
  public static final String CONFIG_CRON_SCHEDULE = "defaultCronSchedule";

  /** Configuration key for default cron timezone (required) */
  public static final String CONFIG_CRON_TIMEZONE = "defaultCronTimezone";

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Monitor Missing";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that native assertions have an associated monitor for execution";
  }

  @Override
  public boolean isOnDemandOnly() {
    return false;
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkAssertion(
      @Nonnull CheckContext ctx,
      @Nonnull Urn assertionUrn,
      @Nonnull EntityResponse response,
      @Nonnull AssertionInfo assertionInfo) {

    // Skip if no entityUrn (handled by AssertionEntityUrnMissingCheck)
    if (!assertionInfo.hasEntityUrn() || assertionInfo.getEntityUrn() == null) {
      return Collections.emptyList();
    }

    Urn entityUrn = assertionInfo.getEntityUrn();

    // Skip if invalid entity type (handled by AssertionEntityTypeInvalidCheck)
    if (!isValidAssertionEntityType(entityUrn.getEntityType())) {
      return Collections.emptyList();
    }

    // Skip external assertions (they don't need monitors)
    if (isExternalAssertion(assertionInfo)) {
      return Collections.emptyList();
    }

    // Check if monitor exists for this assertion
    Urn existingMonitorUrn = getMonitorForAssertion(ctx, assertionUrn);
    if (existingMonitorUrn == null) {
      // Get cron configuration from check context (configured in application.yaml)
      Map<String, String> checkConfig = ctx.getCheckConfig(getId());
      String cronSchedule = checkConfig.get(CONFIG_CRON_SCHEDULE);
      String cronTimezone = checkConfig.get(CONFIG_CRON_TIMEZONE);

      // Skip if cron configuration is not provided
      if (cronSchedule == null || cronTimezone == null) {
        log.warn(
            "Skipping assertion {} - cron configuration not provided. "
                + "Configure consistencyChecks.checks.assertion-monitor-missing.{} and {} in application.yaml",
            assertionUrn,
            CONFIG_CRON_SCHEDULE,
            CONFIG_CRON_TIMEZONE);
        return Collections.emptyList();
      }

      // Generate a deterministic monitor URN based on the assertion
      Urn newMonitorUrn = generateMonitorUrn(entityUrn, assertionUrn);

      // Create batch items for the new monitor
      List<BatchItem> batchItems =
          createMonitorBatchItems(
              ctx, newMonitorUrn, entityUrn, assertionUrn, cronSchedule, cronTimezone);

      return List.of(
          createIssueBuilder(assertionUrn, ConsistencyFixType.CREATE)
              .description(
                  String.format(
                      "Native assertion has no associated monitor. A disabled monitor will be created for entity: %s",
                      entityUrn))
              .relatedUrns(List.of(entityUrn, newMonitorUrn))
              .batchItems(batchItems)
              .build());
    }

    return Collections.emptyList();
  }

  /**
   * Check if an assertion is external and therefore should NOT require a monitor.
   *
   * @param assertionInfo the assertion info to check
   * @return true if the assertion is external
   */
  private boolean isExternalAssertion(AssertionInfo assertionInfo) {
    // Check assertion type first - DATASET and CUSTOM are always external
    if (assertionInfo.hasType()) {
      AssertionType type = assertionInfo.getType();
      if (AssertionType.DATASET.equals(type) || AssertionType.CUSTOM.equals(type)) {
        return true;
      }
    }

    // Check source type - only NATIVE and INFERRED assertions require monitors
    if (assertionInfo.hasSource() && assertionInfo.getSource().hasType()) {
      AssertionSourceType sourceType = assertionInfo.getSource().getType();
      // If source is NATIVE or INFERRED, it's not external
      return !AssertionSourceType.NATIVE.equals(sourceType)
          && !AssertionSourceType.INFERRED.equals(sourceType);
    }

    // No source defined - treat as external (legacy assertions before source field existed)
    return true;
  }

  /**
   * Get the monitor URN associated with an assertion.
   *
   * @param ctx check context
   * @param assertionUrn URN of the assertion
   * @return monitor URN if found, null otherwise
   */
  @Nullable
  private Urn getMonitorForAssertion(CheckContext ctx, Urn assertionUrn) {
    try {
      EntityRelationships relationships =
          ctx.getGraphClient()
              .getRelatedEntities(
                  assertionUrn.toString(),
                  ImmutableSet.of(EVALUATES_RELATIONSHIP_NAME),
                  RelationshipDirection.INCOMING,
                  0,
                  1,
                  null);

      if (relationships.getRelationships() != null && !relationships.getRelationships().isEmpty()) {
        return relationships.getRelationships().get(0).getEntity();
      }
    } catch (Exception e) {
      log.warn("Error getting monitor for assertion {}: {}", assertionUrn, e.getMessage());
    }

    return null;
  }

  /**
   * Generate a deterministic monitor URN based on the entity and assertion.
   *
   * <p>Uses the assertion's ID to ensure a 1:1 mapping between assertions and monitors.
   *
   * @param entityUrn the entity URN (dataset, dataJob, etc.)
   * @param assertionUrn the assertion URN
   * @return the generated monitor URN
   */
  private Urn generateMonitorUrn(Urn entityUrn, Urn assertionUrn) {
    return MonitorUrnUtils.generateMonitorUrn(entityUrn, assertionUrn.getId());
  }

  /**
   * Create batch items for a new monitor entity.
   *
   * @param ctx check context
   * @param monitorUrn the URN for the new monitor
   * @param entityUrn the entity URN (dataset, dataJob, etc.)
   * @param assertionUrn the assertion URN
   * @param cronSchedule cron schedule string
   * @param cronTimezone cron timezone
   * @return list of batch items to create the monitor
   */
  private List<BatchItem> createMonitorBatchItems(
      CheckContext ctx,
      Urn monitorUrn,
      Urn entityUrn,
      Urn assertionUrn,
      String cronSchedule,
      String cronTimezone) {

    List<BatchItem> batchItems = new ArrayList<>();

    // Create MonitorInfo aspect (key aspect is auto-generated from URN)
    MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);

    // Set status to INACTIVE (disabled) - user must enable it manually
    MonitorStatus status = new MonitorStatus();
    status.setMode(MonitorMode.INACTIVE);
    monitorInfo.setStatus(status);

    // Create AssertionMonitor with the assertion reference
    AssertionMonitor assertionMonitor = new AssertionMonitor();

    // Create the assertion evaluation spec
    AssertionEvaluationSpec spec = new AssertionEvaluationSpec();
    spec.setAssertion(assertionUrn);

    // Set the cron schedule
    CronSchedule schedule = new CronSchedule();
    schedule.setCron(cronSchedule);
    schedule.setTimezone(cronTimezone);
    spec.setSchedule(schedule);

    assertionMonitor.setAssertions(
        new AssertionEvaluationSpecArray(Collections.singletonList(spec)));
    monitorInfo.setAssertionMonitor(assertionMonitor);

    batchItems.add(
        createUpsertItem(
            ctx, monitorUrn, MONITOR_INFO_ASPECT_NAME, monitorInfo, ChangeType.CREATE));

    return batchItems;
  }
}
