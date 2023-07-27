package com.linkedin.metadata.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.CronSchedule;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionEvaluationSpecArray;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.monitor.MonitorMode;
import com.linkedin.monitor.MonitorStatus;
import com.linkedin.monitor.MonitorType;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.datahub.authentication.Authentication;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.MonitorKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;



@Slf4j
public class MonitorService extends BaseService {

  private static final MonitorType DEFAULT_MONITOR_TYPE = MonitorType.ASSERTION;

  public MonitorService(@Nonnull final EntityClient entityClient, @Nonnull final Authentication systemAuthentication) {
    super(entityClient, systemAuthentication);
  }

  /**
   * Returns an instance of {@link MonitorInfo} for the specified Monitor urn,
   * or null if one cannot be found.
   *
   * @param monitorUrn the urn of the Monitor
   *
   * @return an instance of {@link com.linkedin.monitor.MonitorInfo} for the Monitor, null if it does not exist.
   */
  @Nullable
  public MonitorInfo getMonitorInfo(@Nonnull final Urn monitorUrn) {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    final EntityResponse response = getMonitorEntityResponse(monitorUrn, this.systemAuthentication);
    if (response != null && response.getAspects().containsKey(Constants.MONITOR_INFO_ASPECT_NAME)) {
      return new MonitorInfo(response.getAspects().get(Constants.MONITOR_INFO_ASPECT_NAME).getValue().data());
    }
    // No aspect found
    return null;
  }

  /**
   * Returns an instance of {@link EntityResponse} for the specified View urn,
   * or null if one cannot be found.
   *
   * @param monitorUrn the urn of the View
   * @param authentication the authentication to use
   *
   * @return an instance of {@link EntityResponse} for the View, null if it does not exist.
   */
  @Nullable
  public EntityResponse getMonitorEntityResponse(@Nonnull final Urn monitorUrn, @Nonnull final Authentication authentication) {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");
    try {
      return this.entityClient.getV2(
          Constants.MONITOR_ENTITY_NAME,
          monitorUrn,
          ImmutableSet.of(Constants.MONITOR_INFO_ASPECT_NAME),
          authentication
      );
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to retrieve Monitor with urn %s", monitorUrn), e);
    }
  }

  /**
   * Creates a new Dataset or DataJob Freshness Monitor for native execution by DataHub.
   * Assumes that the caller has already performed the required authorization.
   *
   * Throws an exception if the provided assertion urn does not exist.
   */
  @Nonnull
  public Urn createAssertionMonitor(
      @Nonnull final Urn entityUrn,
      @Nonnull final Urn assertionUrn,
      @Nonnull final CronSchedule schedule,
      @Nonnull final AssertionEvaluationParameters parameters,
      @Nonnull final Authentication authentication) throws Exception {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    Objects.requireNonNull(assertionUrn, "assertionUrn must not be null");
    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(parameters, "parameters must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    // Verify that the target entity actually exists.
    validateEntity(entityUrn, authentication);

    // Verify that the target assertion actually exists.
    validateEntity(assertionUrn, authentication);

    final Urn monitorUrn = generateMonitorUrn(entityUrn);

    final MonitorInfo monitorInfo = new MonitorInfo();
    monitorInfo.setType(MonitorType.ASSERTION);

    // New monitors will default to 'Active' mode.
    monitorInfo.setStatus(new MonitorStatus().setMode(MonitorMode.ACTIVE));

    final AssertionMonitor assertionMonitor = new AssertionMonitor();
    assertionMonitor.setAssertions(new AssertionEvaluationSpecArray(
        ImmutableList.of(
            new AssertionEvaluationSpec()
                .setAssertion(assertionUrn)
                .setSchedule(schedule)
                .setParameters(parameters)
        )
    ));
    monitorInfo.setAssertionMonitor(assertionMonitor);

    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, monitorInfo));

    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to create new Monitor for Assertion with urn %s", assertionUrn), e);
    }
  }

  /**
   * Upserts the mode of a particular monitor, to enable or disable it's operation.
   *
   * If the monitor does not yet have an info aspect, a default will be minted for the provided urn.
   */
  @Nonnull
  public Urn upsertMonitorMode(
      @Nonnull final Urn monitorUrn,
      @Nonnull final MonitorMode monitorMode,
      @Nonnull final Authentication authentication) throws Exception {
    Objects.requireNonNull(monitorUrn, "monitorUrn must not be null");
    Objects.requireNonNull(monitorMode, "monitorMode must not be null");
    Objects.requireNonNull(authentication, "authentication must not be null");

    MonitorInfo info = getMonitorInfo(monitorUrn);

    // If monitor info does not yet exist, then mint a default info aspect
    if (info == null) {
      info = new MonitorInfo()
          .setType(DEFAULT_MONITOR_TYPE)
          .setAssertionMonitor(
              new AssertionMonitor().setAssertions(new AssertionEvaluationSpecArray(Collections.emptyList())));
    }

    // Update the status to have the new Monitor Mode
    info.setStatus(new MonitorStatus().setMode(monitorMode));

    // Write the info back!
    final List<MetadataChangeProposal> aspects = new ArrayList<>();
    aspects.add(AspectUtils.buildMetadataChangeProposal(monitorUrn, Constants.MONITOR_INFO_ASPECT_NAME, info));

    try {
      this.entityClient.batchIngestProposals(
          aspects,
          authentication,
          false
      );
      return monitorUrn;
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failed to upsert Monitor Mode for monitor with urn %s", monitorUrn), e);
    }
  }

  private void validateEntity(@Nonnull final Urn entityUrn, @Nonnull final Authentication authentication) throws Exception {
    if (!this.entityClient.exists(entityUrn, authentication)) {
      throw new IllegalArgumentException(String.format("Failed to edit Monitor. %s with urn %s does not exist.", entityUrn
          .getEntityType(), entityUrn));
    }
  }

  @Nonnull
  private Urn generateMonitorUrn(@Nonnull final Urn entityUrn) {
    final MonitorKey key = new MonitorKey();
    final String id = UUID.randomUUID().toString();
    key.setEntity(entityUrn);
    key.setId(id);
    return EntityKeyUtils.convertEntityKeyToUrn(key, Constants.MONITOR_ENTITY_NAME);
  }
}