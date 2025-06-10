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
import javax.annotation.Nonnull;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Import;
import org.springframework.stereotype.Component;

/**
 * This hook looks for changes in the formAssignmentStatus aspect. It looks to see if a form has
 * just completed assignment, and if the form is published. If yes to both of these, trigger
 * notifications for the form.
 */
@Slf4j
@Component
@Import({SystemAuthenticationFactory.class})
public class FormAssignmentStatusHook implements MetadataChangeLogHook {
  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.RESTATE);

  private final boolean isEnabled;
  private final SystemEntityClient systemEntityClient;

  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public FormAssignmentStatusHook(
      @Nonnull @Value("${forms.hook.assignmentStatusEnabled:true}") Boolean isEnabled,
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${forms.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.isEnabled = isEnabled;
    this.systemEntityClient =
        Objects.requireNonNull(systemEntityClient, "systemEntityClient is required");
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public FormAssignmentStatusHook(
      @Nonnull Boolean isEnabled, @Nonnull final SystemEntityClient systemEntityClient) {
    this(isEnabled, systemEntityClient, "");
  }

  @Override
  public FormAssignmentStatusHook init(@Nonnull OperationContext systemOperationContext) {
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
      handleSendFormNotifications(event);
    }
  }

  /**
   * Check if form is asset assignment is complete and if the form is published. If yes, send
   * notifications.
   */
  private void handleSendFormNotifications(@Nonnull final MetadataChangeLog event) {
    Urn formUrn = event.getEntityUrn();
    // 1. Get the new formAssignmentStatus aspect
    final FormAssignmentStatus formAssignmentStatus =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            FormAssignmentStatus.class);

    if (!formAssignmentStatus.getStatus().equals(AssignmentStatus.COMPLETE)) {
      return;
    }
    // 2. If status is complete, fetch formInfo to see if form is published
    boolean isFormPublished = false;
    try {
      EntityResponse response =
          systemEntityClient.getV2(
              systemOperationContext, formUrn, ImmutableSet.of(FORM_INFO_ASPECT_NAME));
      if (response != null && response.getAspects().containsKey(FORM_INFO_ASPECT_NAME)) {
        final FormInfo formInfo =
            new FormInfo(response.getAspects().get(FORM_INFO_ASPECT_NAME).getValue().data());
        isFormPublished = formInfo.getStatus().getState().equals(FormState.PUBLISHED);
      }
    } catch (Exception e) {
      log.error("Issue fetching formAssignmentStatus aspect to trigger form notifications", e);
    }

    // 3. if form is published, trigger notifications for form
    if (isFormPublished) {
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
        && FORM_ASSIGNMENT_STATUS_ASPECT_NAME.equals(event.getAspectName());
  }
}
