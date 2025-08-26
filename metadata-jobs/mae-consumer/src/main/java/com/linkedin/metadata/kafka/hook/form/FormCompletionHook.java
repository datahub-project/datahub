package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.form.FormServiceFactory;
import com.linkedin.metadata.aspect.patch.builder.FormsPatchBuilder;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
 * This hook looks for changes in the forms aspect of entities and determines whether the different
 * forms in completedForms or incompleteForms should be updated to be complete/incomplete.
 *
 * <p>This hook was originally built because async bulk form submission patches form response
 * completion, but can't handle updating a form to be complete once all the required prompts are
 * completed.
 */
@Slf4j
@Component
@Import({FormServiceFactory.class, SystemAuthenticationFactory.class})
public class FormCompletionHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(ChangeType.UPSERT, ChangeType.RESTATE);

  private final FormService formService;
  private final boolean isEnabled;
  private final SystemEntityClient systemEntityClient;

  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public FormCompletionHook(
      @Nonnull final FormService formService,
      @Nonnull @Value("${forms.hook.completionEnabled:true}") Boolean isEnabled,
      @Nonnull final SystemEntityClient systemEntityClient,
      @Nonnull @Value("${forms.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.formService = Objects.requireNonNull(formService, "formService is required");
    this.isEnabled = isEnabled;
    this.systemEntityClient =
        Objects.requireNonNull(systemEntityClient, "systemEntityClient is required");
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public FormCompletionHook(
      @Nonnull final FormService formService,
      @Nonnull Boolean isEnabled,
      @Nonnull final SystemEntityClient systemEntityClient) {
    this(formService, isEnabled, systemEntityClient, "");
  }

  @Override
  public FormCompletionHook init(@Nonnull OperationContext systemOperationContext) {
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
      handleFormsAspectUpdate(event);
    }
  }

  /** Handle a forms update on an asset by checking if a form is complete */
  private void handleFormsAspectUpdate(@Nonnull final MetadataChangeLog event) {

    // 1. Get the new forms aspect
    final Forms forms =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(), event.getAspect().getContentType(), Forms.class);

    // 2. Gather all the form urns and batch fetch them to get their info
    final Set<Urn> formUrns = new HashSet<>();
    forms.getIncompleteForms().forEach(form -> formUrns.add(form.getUrn()));
    forms.getCompletedForms().forEach(form -> formUrns.add(form.getUrn()));
    Map<Urn, FormInfo> formInfoMap =
        formService.batchFetchForms(this.systemOperationContext, formUrns);

    // 3. Get lists of forms we need to mark as complete or incomplete
    // We should only move forms automatically to complete if at least one prompt is complete
    List<FormAssociation> formsToMarkComplete = new ArrayList<>();
    forms
        .getIncompleteForms()
        .forEach(
            incompleteForm -> {
              FormInfo formInfo = formInfoMap.get(incompleteForm.getUrn());
              if (formInfo != null
                  && formService.isFormCompleted(incompleteForm, formInfo)
                  && incompleteForm.getCompletedPrompts().size() > 0) {
                formsToMarkComplete.add(incompleteForm);
              }
            });

    List<FormAssociation> formsToMarkIncomplete = new ArrayList<>();
    forms
        .getCompletedForms()
        .forEach(
            completedForm -> {
              FormInfo formInfo = formInfoMap.get(completedForm.getUrn());
              if (formInfo != null && !formService.isFormCompleted(completedForm, formInfo)) {
                formsToMarkIncomplete.add(completedForm);
              }
            });

    if (formsToMarkComplete.size() == 0 && formsToMarkIncomplete.size() == 0) {
      return;
    }

    // 4. Write patch MCP to update forms as complete or incomplete
    FormsPatchBuilder patchBuilder = new FormsPatchBuilder().urn(event.getEntityUrn());
    formsToMarkComplete.forEach(patchBuilder::completeForm);
    formsToMarkIncomplete.forEach(patchBuilder::setFormIncomplete);
    MetadataChangeProposal mcp = patchBuilder.build();

    try {
      systemEntityClient.ingestProposal(systemOperationContext, mcp, true);
    } catch (Exception e) {
      log.error(
          String.format(
              "Failed to emit form completion updates for entity %s", event.getEntityUrn()),
          e);
    }
  }

  /** Returns true if the event should be processed */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isFormsAspectUpdated(event);
  }

  /** Returns true if the event represents an update the prompt set of a form. */
  private boolean isFormsAspectUpdated(@Nonnull final MetadataChangeLog event) {
    return SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && FORMS_ASPECT_NAME.equals(event.getAspectName());
  }
}
