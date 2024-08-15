package com.linkedin.metadata.kafka.hook.form;

import static com.linkedin.metadata.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.DynamicFormAssignment;
import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.gms.factory.form.FormServiceFactory;
import com.linkedin.metadata.kafka.hook.MetadataChangeLogHook;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeLog;
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
 * This hook is used for assigning / un-assigning forms for specific entities.
 *
 * <p>Specifically, this hook performs the following operations:
 *
 * <p>1. When a new dynamic form assignment is created, an automation (metadata test) with the form
 * urn embedded is automatically generated, which is responsible for assigning the form to any
 * entities in the target set. It also will attempt a removal of the form for any failing entities.
 *
 * <p>2. When a new form is created, or an existing one updated, automations (metadata tests) will
 * be generated for each prompt in the metadata test which verifies that the entities with that test
 * associated with it are complying with the prompt. When they are NOT, the test will mark the
 * prompts as incomplete.
 *
 * <p>3. When a form is hard deleted, any automations used for assigning the form, or validating
 * prompts, are automatically deleted.
 *
 * <p>TODO: In the future, let's decide whether we want to support automations to auto-mark form
 * prompts as "completed" when they do in fact have the correct metadata. (Without user needing to
 * explicitly fill out a form prompt response)
 *
 * <p>TODO: Write a unit test for this class.
 */
@Slf4j
@Component
@Import({FormServiceFactory.class, SystemAuthenticationFactory.class})
public class FormAssignmentHook implements MetadataChangeLogHook {

  private static final Set<ChangeType> SUPPORTED_UPDATE_TYPES =
      ImmutableSet.of(
          ChangeType.UPSERT, ChangeType.CREATE, ChangeType.CREATE_ENTITY, ChangeType.RESTATE);

  private final FormService formService;
  private final boolean isEnabled;

  private OperationContext systemOperationContext;
  @Getter private final String consumerGroupSuffix;

  @Autowired
  public FormAssignmentHook(
      @Nonnull final FormService formService,
      @Nonnull @Value("${forms.hook.enabled:true}") Boolean isEnabled,
      @Nonnull @Value("${forms.hook.consumerGroupSuffix}") String consumerGroupSuffix) {
    this.formService = Objects.requireNonNull(formService, "formService is required");
    this.isEnabled = isEnabled;
    this.consumerGroupSuffix = consumerGroupSuffix;
  }

  @VisibleForTesting
  public FormAssignmentHook(@Nonnull final FormService formService, @Nonnull Boolean isEnabled) {
    this(formService, isEnabled, "");
  }

  @Override
  public FormAssignmentHook init(@Nonnull OperationContext systemOperationContext) {
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
      if (isFormDynamicFilterUpdated(event)) {
        handleFormFilterUpdated(event);
      }
    }
  }

  /** Handle an form filter update by adding updating the targeting automation for it. */
  private void handleFormFilterUpdated(@Nonnull final MetadataChangeLog event) {
    // 1. Get the new form assignment
    DynamicFormAssignment formFilters =
        GenericRecordUtils.deserializeAspect(
            event.getAspect().getValue(),
            event.getAspect().getContentType(),
            DynamicFormAssignment.class);

    // 2. Register a automation to assign it.
    formService.upsertFormAssignmentRunner(
        systemOperationContext, event.getEntityUrn(), formFilters);
  }

  /**
   * Returns true if the event should be processed, which is only true if the change is on the
   * incident status aspect
   */
  private boolean isEligibleForProcessing(@Nonnull final MetadataChangeLog event) {
    return isFormPromptSetUpdated(event)
        || isFormDynamicFilterUpdated(event)
        || isFormDeleted(event);
  }

  /** Returns true if an form is being hard-deleted. */
  private boolean isFormDeleted(@Nonnull final MetadataChangeLog event) {
    return FORM_ENTITY_NAME.equals(event.getEntityType())
        && ChangeType.DELETE.equals(event.getChangeType())
        && FORM_KEY_ASPECT_NAME.equals(event.getAspectName());
  }

  /** Returns true if the event represents an update the prompt set of a form. */
  private boolean isFormPromptSetUpdated(@Nonnull final MetadataChangeLog event) {
    return FORM_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && FORM_INFO_ASPECT_NAME.equals(event.getAspectName());
  }

  /** Returns true if the event represents an update to the dynamic filter for a form. */
  private boolean isFormDynamicFilterUpdated(@Nonnull final MetadataChangeLog event) {
    return FORM_ENTITY_NAME.equals(event.getEntityType())
        && SUPPORTED_UPDATE_TYPES.contains(event.getChangeType())
        && DYNAMIC_FORM_ASSIGNMENT_ASPECT_NAME.equals(event.getAspectName());
  }
}
