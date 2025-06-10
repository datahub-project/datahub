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
 * This hook looks for changes in the formInfo aspect to determine if we should notify assignees
 * once the form becomes published.
 */
@Slf4j
@Component
@Import({SystemAuthenticationFactory.class})
public class FormInfoHook implements MetadataChangeLogHook {
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
  public FormInfoHook(
      @Nonnull @Value("${forms.hook.infoEnabled:true}") Boolean isEnabled,
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${forms.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.isEnabled = isEnabled;
    this.systemEntityClient =
        Objects.requireNonNull(systemEntityClient, "systemEntityClient is required");
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public FormInfoHook(
      @Nonnull Boolean isEnabled, @Nonnull final SystemEntityClient systemEntityClient) {
    this(isEnabled, systemEntityClient, "");
  }

  @Override
  public FormInfoHook init(@Nonnull OperationContext systemOperationContext) {
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
              log.error("Error handling formInfo MCL hook", e);
            }
          };
      // run this in async thread to not block operations that update formAssignmentStatus if
      // assigning and publishing simultaneously
      Thread assignThread =
          new Thread(runnable, FormInfoHook.class.getSimpleName() + nextThreadNum());
      assignThread.start();
    }
  }

  /**
   * Check if form is newly Published, and if we are not assigning to assets, trigger forms
   * notifications ingestion job.
   */
  private void handleSendFormNotifications(@Nonnull final MetadataChangeLog event) {
    Urn formUrn = event.getEntityUrn();
    final FormInfo formInfo =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), FormInfo.class);

    FormInfo prevFormInfo = null;
    if (event.getPreviousAspectValue() != null) {
      prevFormInfo =
          GenericRecordUtils.deserializeAspect(
              event.getPreviousAspectValue().getValue(),
              event.getPreviousAspectValue().getContentType(),
              FormInfo.class);
    }

    // 1. Check if form is now published and was not published before
    boolean isNowPublished = formInfo.getStatus().getState().equals(FormState.PUBLISHED);
    boolean wasPreviouslyPublished =
        prevFormInfo != null && prevFormInfo.getStatus().getState().equals(FormState.PUBLISHED);

    if (isNowPublished && !wasPreviouslyPublished) {
      Boolean isCurrentlyAssigning = false;
      try {
        // sleep for 3 seconds to wait for formAssignmentStatus to update if publishing this form
        // and assigning at the same time
        Thread.sleep(3000);
        // 2. Check to see if this form is currently assigning to assets
        EntityResponse response =
            systemEntityClient.getV2(
                systemOperationContext,
                formUrn,
                ImmutableSet.of(FORM_ASSIGNMENT_STATUS_ASPECT_NAME));
        if (response != null
            && response.getAspects().containsKey(FORM_ASSIGNMENT_STATUS_ASPECT_NAME)) {
          final FormAssignmentStatus formAssignmentStatus =
              new FormAssignmentStatus(
                  response.getAspects().get(FORM_ASSIGNMENT_STATUS_ASPECT_NAME).getValue().data());
          isCurrentlyAssigning =
              formAssignmentStatus.getStatus().equals(AssignmentStatus.IN_PROGRESS);
        }
      } catch (Exception e) {
        log.error(
            "Issue waiting and fetching formAssignmentStatus aspect to trigger form notifications",
            e);
      }

      if (isCurrentlyAssigning) {
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
        && FORM_INFO_ASPECT_NAME.equals(event.getAspectName());
  }
}
