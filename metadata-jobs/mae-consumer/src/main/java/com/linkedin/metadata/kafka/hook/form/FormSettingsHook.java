package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.AssignmentStatus;
import com.linkedin.form.FormAssignmentStatus;
import com.linkedin.form.FormInfo;
import com.linkedin.form.FormSettings;
import com.linkedin.form.FormState;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * This hook looks for changes in the formSettings aspect to determine if we should notify assignees
 * once the form becomes published.
 */
@Slf4j
@Component
@Import({SystemAuthenticationFactory.class})
public class FormSettingsHook implements MetadataChangeLogHook {
  private static final AtomicLong THREAD_INIT_NUMBER = new AtomicLong();

  private static long nextThreadNum() {
    return THREAD_INIT_NUMBER.getAndIncrement();
  }

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.RESTATE);

  private final boolean isEnabled;
  private final SystemEntityClient systemEntityClient;

  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public FormSettingsHook(
      @Nonnull @Value("${forms.hook.settingsEnabled:true}") Boolean isEnabled,
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${forms.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.isEnabled = isEnabled;
    this.systemEntityClient =
        Objects.requireNonNull(systemEntityClient, "systemEntityClient is required");
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public FormSettingsHook(
      @Nonnull Boolean isEnabled, @Nonnull final SystemEntityClient systemEntityClient) {
    this(isEnabled, systemEntityClient, "");
  }

  @Override
  public FormSettingsHook init(@Nonnull OperationContext systemOperationContext) {
    this.systemOperationContext = systemOperationContext;
    return this;
  }

  @Override
  public boolean isEnabled() {
    return isEnabled;
  }

  @Override
  public void invoke(@Nonnull final MetadataChangeLog event) {
    if (isEnabled && isEligibleForProcessing(event)) {
      Runnable runnable =
          () -> {
            try {
              handleSendFormNotifications(event);
            } catch (Exception e) {
              log.error("Error handling formSettings MCL hook", e);
            }
          };
      // run this in async thread to not block operations that update formAssignmentStatus if
      // assigning and updating simultaneously
      Thread assignThread =
          new Thread(runnable, FormSettingsHook.class.getSimpleName() + nextThreadNum());
      assignThread.start();
    }
  }

  /**
   * Check if form is newly Published, and if we are not assigning to assets, trigger forms
   * notifications ingestion job.
   */
  private void handleSendFormNotifications(@Nonnull final MetadataChangeLog event) {
    Urn formUrn = event.getEntityUrn();
    final FormSettings formSettings =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), FormSettings.class);

    FormSettings prevFormSettings = null;
    if (event.getPreviousAspectValue() != null) {
      prevFormSettings =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              FormSettings.class);
    }

    // 1. Check conditions on previous settings compared to new settings
    boolean isNowNotifyingOnPublish =
        formSettings.getNotificationSettings().isNotifyAssigneesOnPublish();
    boolean wasNotifyingOnPublish =
        prevFormSettings != null
            && prevFormSettings.getNotificationSettings().isNotifyAssigneesOnPublish();

    // if we are newly published, or if actors have changed and we are published, attempt to notify
    if (isNowNotifyingOnPublish && !wasNotifyingOnPublish) {
      boolean isCurrentlyAssigning = false;
      boolean isCurrentlyPublished = false;
      try {
        // sleep for 3 seconds to wait for formAssignmentStatus and formInfo to update
        // if we are updating all of these at the same time
        Thread.sleep(3000);
        EntityResponse response =
            systemEntityClient.getV2(
                systemOperationContext,
                formUrn,
                ImmutableSet.of(FORM_INFO_ASPECT_NAME, FORM_ASSIGNMENT_STATUS_ASPECT_NAME));
        // 2. Check to see if this form is currently assigning to assets
        if (response != null
            && response.getAspects().containsKey(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)) {
          final FormAssignmentStatus formAssignmentStatus =
              new FormAssignmentStatus(
                  response.getAspects().get(FORM_ASSIGNMENT_STATUS_ASPECT_NAME).getValue().data());
          isCurrentlyAssigning =
              formAssignmentStatus.getStatus().equals(AssignmentStatus.IN_PROGRESS);
        }
        // 3. Check to see if this form is currently published
        if (response != null && response.getAspects().containsKey(FORM_INFO_ASPECT_NAME)) {
          final FormInfo formInfo =
              new FormInfo(response.getAspects().get(FORM_INFO_ASPECT_NAME).getValue().data());
          isCurrentlyPublished = formInfo.getStatus().getState().equals(FormState.PUBLISHED);
        }
      } catch (Exception e) {
        log.error(
            "Issue waiting and fetching formInfo and formAssignmentStatus aspect to trigger form notifications",
            e);
      }

      if (isCurrentlyAssigning || !isCurrentlyPublished) {
        return;
      }

      // 3. trigger notifications for this form if not currently assigning
      MetadataChangeProposal mcp =
          FormUtils.createFormNotificationsExecutionRequest(formUrn.toString());
      try {
        // create synchronous MCP to kick off form notifications ingestion right away
        systemEntityClient.ingestProposal(systemOperationContext, mcp, false);
      } catch (Exception e) {
        log.error(
            String.format(
                "Failed to emit form notification updates for entity %s", event.getEntityUrn()),
            e);
      }
    }
  }

  /** Returns true if the event should be processed */
  boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isFormInfoAspectUpdated(event);
  }

  /** Returns true if the event represents an update to the formInfo aspect */
  private boolean isFormInfoAspectUpdated(@Nonnull final MetadataChangeLog event) {
    return SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && FORM_SETTINGS_ASPECT_NAME.equals(event.getAspectName());
  }
}
