package com.linkedin.metadata.service;

import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_ACCEPTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_RESULT_REJECTED;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_COMPLETE;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_STATUS_PENDING;
import static com.linkedin.metadata.AcrylConstants.ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST;
import static com.linkedin.metadata.Constants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.actionrequest.ActionRequestStatus;
import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.actionworkflow.ActionWorkflowField;
import com.linkedin.actionworkflow.ActionWorkflowFieldCondition;
import com.linkedin.actionworkflow.ActionWorkflowFieldConditionType;
import com.linkedin.actionworkflow.ActionWorkflowFormRequest;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestField;
import com.linkedin.actionworkflow.ActionWorkflowFormRequestFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.actionworkflow.ActionWorkflowRequestAccess;
import com.linkedin.actionworkflow.ActionWorkflowRequestStepState;
import com.linkedin.actionworkflow.ActionWorkflowStep;
import com.linkedin.actionworkflow.ActionWorkflowStepActors;
import com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment;
import com.linkedin.actionworkflow.ActionWorkflowStepType;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.key.ActionRequestKey;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.SortCriterion;
import com.linkedin.metadata.query.filter.SortOrder;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.structured.PropertyCardinality;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/**
 * This class is used to permit easy CRUD operations on ActionRequest entities with workflow
 * requests. Currently it supports creating workflow requests as part of ActionRequest entities.
 *
 * <p>Note that no Authorization is performed within the service. The expectation is that the caller
 * has already verified the permissions of the active Actor.
 *
 * <p>Validation is handled automatically by the ActionWorkflowFormRequestValidator when the entity
 * is inserted into the database.
 */
@Slf4j
public class ActionWorkflowService extends BaseService {

  private final EntityClient _entityClient;
  private final OwnerService _ownerService; // Used for fetching entity owners.
  private final DomainService _domainService; // Used for fetching entity domains.
  private final DataProductService
      _dataProductService; // Used for fetching entity data product. We might need to change this
  private final UserService _userService; // Used for resolving user groups and roles.

  public ActionWorkflowService(
      @Nonnull SystemEntityClient entityClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper,
      @Nonnull final OwnerService ownerService,
      @Nonnull final DomainService domainService,
      @Nonnull final DataProductService dataProductService,
      @Nonnull final UserService userService) {
    super(entityClient, openApiClient, objectMapper);
    this._entityClient = entityClient;
    this._ownerService = ownerService;
    this._domainService = domainService;
    this._dataProductService = dataProductService;
    this._userService = userService;
  }

  /**
   * Creates a new action request workflow request.
   *
   * @param workflowUrn the workflow URN
   * @param entityUrn the target entity URN
   * @param description the request description
   * @param fields the field values
   * @param expiresAt the expiration time
   * @param opContext the operation context
   * @return the URN of the created action request
   * @throws Exception if the operation fails
   */
  public Urn createActionWorkflowFormRequest(
      @Nullable Urn workflowUrn, // the workflow this request belongs to
      @Nullable Urn entityUrn, // the target entity being requested access for.
      @Nullable String description,
      @Nonnull List<ActionWorkflowFormRequestField> fields,
      @Nullable Long expiresAt,
      @Nonnull OperationContext opContext)
      throws Exception {
    Objects.requireNonNull(workflowUrn, "workflowUrn must not be null");
    Objects.requireNonNull(fields, "fields must not be null");
    Objects.requireNonNull(opContext.getSessionAuthentication(), "authentication must not be null");

    // When creating a new action request workflow request, we need to find and assign the current
    // users who
    // need to approve the request. Eventually, we'll need to store an "activity log" that emits key
    // events when
    // steps are completed which we can show in a timeline.

    // First, verify that the workflow with the provided urn exists.
    ActionWorkflowInfo workflowInfo = getActionWorkflow(opContext, workflowUrn);

    // Create the workflow request early for validation
    final ActionWorkflowFormRequest workflowRequest = new ActionWorkflowFormRequest();
    workflowRequest.setFields(new ActionWorkflowFormRequestFieldArray(fields));
    workflowRequest.setCategory(workflowInfo.getCategory());
    workflowRequest.setCustomCategory(workflowInfo.getCustomCategory(), SetMode.IGNORE_NULL);
    workflowRequest.setWorkflow(workflowUrn);

    // Set expiresAt in the access field if provided
    if (expiresAt != null) {
      ActionWorkflowRequestAccess access = new ActionWorkflowRequestAccess();
      access.setExpiresAt(expiresAt);
      workflowRequest.setAccess(access);
    }

    // Validate the workflow request against the workflow definition
    validateWorkflowFormRequest(workflowRequest, workflowInfo);

    // Next, get the steps, and use this to dynamically resolve the correct owners if necessary.
    // If the workflow has dynamic assignment, we will resolve the actors based on the expression
    // type.
    // If the workflow has static assignment, we will use the assigned users/groups/roles directly.
    final ActionWorkflowStep firstStep =
        workflowInfo.getSteps().stream()
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            "Invalid workflow found with urn %s. Workflow must have at least one step",
                            workflowUrn)));

    // This should resolve from the first step's actors, OR by dynamically looking up the actor
    // based on the entityUrn (e.g. fetch the entity's owners, or domain owners).
    ActionWorkflowRequestActors actorAssignment =
        resolveActionWorkflowActors(entityUrn, firstStep.getActors(), opContext);

    // Now, create and emit the ActionRequestInfo. Please wrap creating of the ActionRequestInfo in
    // a separate method.

    // 1. Generate a unique id for the new action request
    final ActionRequestKey key = new ActionRequestKey();
    key.setId(UUID.randomUUID().toString());

    // 2. Create the action request params with the workflow request
    final ActionRequestParams params = new ActionRequestParams();
    params.setWorkflowFormRequest(workflowRequest);

    // 3. Create the action request info
    final ActionRequestInfo actionRequestInfo = new ActionRequestInfo();
    actionRequestInfo.setType(ACTION_REQUEST_TYPE_WORKFLOW_FORM_REQUEST);
    actionRequestInfo.setAssignedUsers(new UrnArray(actorAssignment.getUsers()));
    actionRequestInfo.setAssignedGroups(new UrnArray(actorAssignment.getGroups()));
    actionRequestInfo.setAssignedRoles(new UrnArray(actorAssignment.getRoles()));
    actionRequestInfo.setParams(params);
    actionRequestInfo.setDescription(description, SetMode.IGNORE_NULL);
    actionRequestInfo.setCreated(System.currentTimeMillis());
    actionRequestInfo.setCreatedBy(
        UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()));
    actionRequestInfo.setDescription(description, SetMode.IGNORE_NULL);
    actionRequestInfo.setResource(
        entityUrn != null ? entityUrn.toString() : null, SetMode.IGNORE_NULL);

    // Set the step state to the first step
    ActionWorkflowRequestStepState stepState = new ActionWorkflowRequestStepState();
    stepState.setStepId(firstStep.getId());
    workflowRequest.setStepState(stepState);

    // 4. Create the action request urn
    final Urn actionRequestUrn =
        EntityKeyUtils.convertEntityKeyToUrn(key, ACTION_REQUEST_ENTITY_NAME);

    // 5. Create the action request status with PENDING status
    final ActionRequestStatus actionRequestStatus = new ActionRequestStatus();
    actionRequestStatus.setStatus(ACTION_REQUEST_STATUS_PENDING);
    actionRequestStatus.setLastModified(
        new AuditStamp()
            .setActor(UrnUtils.getUrn(opContext.getSessionAuthentication().getActor().toUrnStr()))
            .setTime(System.currentTimeMillis()));

    // 6. Create both metadata change proposals
    final MetadataChangeProposal actionRequestProposal =
        AspectUtils.buildMetadataChangeProposal(
            actionRequestUrn, ACTION_REQUEST_INFO_ASPECT_NAME, actionRequestInfo);

    final MetadataChangeProposal statusProposal =
        AspectUtils.buildMetadataChangeProposal(
            actionRequestUrn, ACTION_REQUEST_STATUS_ASPECT_NAME, actionRequestStatus);

    // 7. Batch ingest both proposals
    _entityClient.batchIngestProposals(
        opContext, Arrays.asList(actionRequestProposal, statusProposal), false);

    return actionRequestUrn;
  }

  /**
   * Validates a workflow request against its workflow definition.
   *
   * @param workflowRequest the workflow request to validate
   * @param workflowInfo the workflow definition
   * @throws IllegalArgumentException if validation fails
   */
  private void validateWorkflowFormRequest(
      @Nonnull ActionWorkflowFormRequest workflowRequest,
      @Nonnull ActionWorkflowInfo workflowInfo) {

    // Use the static validation method
    validateWorkflowFormRequestAgainstDefinition(workflowRequest, workflowInfo);
  }

  /**
   * Validates a workflow request against its workflow definition. This method can be used by both
   * the service and the validator.
   *
   * @param workflowRequest the workflow request to validate
   * @param workflowInfo the workflow definition
   * @throws IllegalArgumentException if validation fails
   */
  public static void validateWorkflowFormRequestAgainstDefinition(
      @Nonnull ActionWorkflowFormRequest workflowRequest,
      @Nonnull ActionWorkflowInfo workflowInfo) {

    if (workflowInfo.getTrigger().hasForm()) {

      // Validate the form request against the workflow definition
      List<ActionWorkflowField> fields = workflowInfo.getTrigger().getForm().getFields();

      // Basic validation - ensure fields are not empty if workflow defines fields
      if (fields != null && fields.size() > 0) {
        if (workflowRequest.getFields() == null || workflowRequest.getFields().size() == 0) {
          throw new IllegalArgumentException(
              "Workflow request must have at least one field when the workflow definition requires fields");
        }
      }

      // Validate each field against the workflow definition
      if (fields != null && workflowRequest.getFields() != null) {
        validateWorkflowFormRequestFields(workflowRequest, workflowInfo);
      }
    }
  }

  /**
   * Validates workflow request fields against the workflow definition.
   *
   * @param workflowRequest the workflow request
   * @param workflowInfo the workflow definition
   * @throws IllegalArgumentException if validation fails
   */
  private static void validateWorkflowFormRequestFields(
      @Nonnull ActionWorkflowFormRequest workflowRequest,
      @Nonnull ActionWorkflowInfo workflowInfo) {

    // Create a map of field definitions by ID for quick lookup
    final Map<String, ActionWorkflowField> fieldDefinitions = new HashMap<>();
    for (ActionWorkflowField fieldDef : workflowInfo.getTrigger().getForm().getFields()) {
      fieldDefinitions.put(fieldDef.getId(), fieldDef);
    }

    // Create field values map once for condition evaluation
    final Map<String, List<String>> fieldValues = createFieldValuesMap(workflowRequest);

    // Validate each field against the definition
    for (ActionWorkflowFormRequestField field : workflowRequest.getFields()) {
      final ActionWorkflowField fieldDef = fieldDefinitions.get(field.getId());

      if (fieldDef == null) {
        throw new IllegalArgumentException(
            String.format("Field with ID '%s' is not defined in workflow", field.getId()));
      }

      validateFieldAgainstDefinition(field, fieldDef, fieldValues);
    }

    // Check for required fields that are missing
    validateRequiredFormFields(workflowRequest, workflowInfo);
  }

  /**
   * Validates a single field against its definition.
   *
   * @param field the field to validate
   * @param fieldDef the field definition
   * @param fieldValues the current field values for condition evaluation
   * @throws IllegalArgumentException if validation fails
   */
  private static void validateFieldAgainstDefinition(
      @Nonnull ActionWorkflowFormRequestField field,
      @Nonnull ActionWorkflowField fieldDef,
      @Nonnull Map<String, List<String>> fieldValues) {

    final String fieldId = field.getId();
    final int valueCount = field.getValues().size();

    // Validate cardinality
    if (fieldDef.getCardinality() == PropertyCardinality.SINGLE && valueCount > 1) {
      throw new IllegalArgumentException(
          String.format(
              "Field '%s' accepts only one value, but %d were provided", fieldId, valueCount));
    }

    // Validate required fields - only skip if field is hidden by conditions
    if (fieldDef.isRequired() && valueCount == 0) {
      // Check if this field should be hidden based on conditions
      boolean shouldBeVisible = shouldDisplayField(fieldDef, fieldValues);

      if (shouldBeVisible) {
        // Field is visible and required but has no values - this should fail
        throw new IllegalArgumentException(
            String.format("Required field '%s' must have at least one value", fieldId));
      }
      // If field is hidden by conditions, we allow empty values
    }

    // Validate allowed values
    if (fieldDef.getAllowedValues() != null && !fieldDef.getAllowedValues().isEmpty()) {
      for (com.linkedin.structured.PrimitivePropertyValue value : field.getValues()) {
        validateValueAgainstConstraints(value, fieldDef, fieldId);
      }
    }
  }

  /**
   * Validates a field value against allowed value constraints.
   *
   * @param value the value to validate
   * @param fieldDef the field definition
   * @param fieldId the field ID for error messages
   * @throws IllegalArgumentException if validation fails
   */
  private static void validateValueAgainstConstraints(
      @Nonnull com.linkedin.structured.PrimitivePropertyValue value,
      @Nonnull ActionWorkflowField fieldDef,
      @Nonnull String fieldId) {

    if (value.isString()) {
      String strValue = value.getString();
      boolean found = false;

      for (com.linkedin.structured.PropertyValue allowedValue : fieldDef.getAllowedValues()) {
        if (allowedValue.getValue().isString()
            && allowedValue.getValue().getString().equals(strValue)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException(
            String.format(
                "Field '%s' value '%s' is not in the list of allowed values", fieldId, strValue));
      }
    } else if (value.isDouble()) {
      Double numValue = value.getDouble();
      boolean found = false;

      for (com.linkedin.structured.PropertyValue allowedValue : fieldDef.getAllowedValues()) {
        if (allowedValue.getValue().isDouble()
            && allowedValue.getValue().getDouble().equals(numValue)) {
          found = true;
          break;
        }
      }

      if (!found) {
        throw new IllegalArgumentException(
            String.format(
                "Field '%s' value '%s' is not in the list of allowed values", fieldId, numValue));
      }
    }
  }

  /**
   * Validates that all required fields are present in the workflow request.
   *
   * @param workflowRequest the workflow request
   * @param workflowInfo the workflow definition
   * @throws IllegalArgumentException if validation fails
   */
  private static void validateRequiredFormFields(
      @Nonnull ActionWorkflowFormRequest workflowRequest,
      @Nonnull ActionWorkflowInfo workflowInfo) {

    // Create a set of provided field IDs and their values for condition evaluation
    final java.util.Set<String> providedFieldIds =
        workflowRequest.getFields().stream()
            .map(ActionWorkflowFormRequestField::getId)
            .collect(java.util.stream.Collectors.toSet());

    // Create a map of field values for condition evaluation
    final Map<String, List<String>> fieldValues = createFieldValuesMap(workflowRequest);

    // Only validate required fields that should be visible based on their conditions
    for (ActionWorkflowField fieldDef : workflowInfo.getTrigger().getForm().getFields()) {
      if (fieldDef.isRequired() && shouldDisplayField(fieldDef, fieldValues)) {
        if (!providedFieldIds.contains(fieldDef.getId())) {
          throw new IllegalArgumentException(
              String.format(
                  "Required field '%s' is missing from workflow request", fieldDef.getId()));
        }
      }
    }
  }

  /**
   * Creates a map of field values from the workflow request for condition evaluation.
   *
   * @param workflowRequest the workflow request
   * @return a map of field IDs to their string values
   */
  private static Map<String, List<String>> createFieldValuesMap(
      @Nonnull ActionWorkflowFormRequest workflowRequest) {
    final Map<String, List<String>> fieldValues = new HashMap<>();

    if (workflowRequest.getFields() != null) {
      for (ActionWorkflowFormRequestField field : workflowRequest.getFields()) {
        List<String> values = new ArrayList<>();
        for (com.linkedin.structured.PrimitivePropertyValue value : field.getValues()) {
          if (value.isString()) {
            values.add(value.getString());
          } else if (value.isDouble()) {
            values.add(String.valueOf(value.getDouble()));
          }
          // PrimitivePropertyValue only supports string and double types
          // Other types would need to be converted to string format during creation
        }
        fieldValues.put(field.getId(), values);
      }
    }

    return fieldValues;
  }

  /**
   * Evaluates whether a field should be displayed based on its condition and current form values.
   * This mirrors the frontend shouldDisplayField logic.
   *
   * @param field the field to evaluate
   * @param fieldValues the current field values
   * @return true if the field should be displayed, false otherwise
   */
  private static boolean shouldDisplayField(
      @Nonnull ActionWorkflowField field, @Nonnull Map<String, List<String>> fieldValues) {

    // If no condition, always display the field
    if (!field.hasCondition()) {
      return true;
    }

    ActionWorkflowFieldCondition condition = field.getCondition();

    // Currently only SINGLE_FIELD_VALUE is supported
    if (condition.getType() == ActionWorkflowFieldConditionType.SINGLE_FIELD_VALUE) {
      if (!condition.hasSingleFieldValueCondition()) {
        return true;
      }

      com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
          condition.getSingleFieldValueCondition();

      // Get the current values for the referenced field
      List<String> referencedFieldValues =
          fieldValues.getOrDefault(singleCondition.getField(), new ArrayList<>());

      // Filter out null/empty values
      List<String> currentStringValues =
          referencedFieldValues.stream()
              .filter(val -> val != null && !val.trim().isEmpty())
              .collect(java.util.stream.Collectors.toList());

      // Check if any current value matches the condition
      boolean hasMatch =
          evaluateCondition(
              currentStringValues, singleCondition.getValues(), singleCondition.getCondition());

      // Apply negation if specified
      return singleCondition.isNegated() ? !hasMatch : hasMatch;
    }

    // Unknown condition type - default to showing the field
    return true;
  }

  /**
   * Evaluates a condition based on the operator.
   *
   * @param currentValues the current field values
   * @param conditionValues the condition values to match against
   * @param operator the comparison operator
   * @return true if the condition is met, false otherwise
   */
  private static boolean evaluateCondition(
      @Nonnull List<String> currentValues,
      @Nonnull com.linkedin.data.template.StringArray conditionValues,
      @Nonnull Condition operator) {

    if (currentValues.isEmpty() || conditionValues.isEmpty()) {
      return false;
    }

    // Check if any current value matches any condition value based on the operator
    for (String currentValue : currentValues) {
      for (String conditionValue : conditionValues) {
        if (evaluateSingleCondition(currentValue, conditionValue, operator)) {
          return true;
        }
      }
    }

    return false;
  }

  /**
   * Evaluates a single value against a condition value using the specified operator.
   *
   * @param currentValue the current field value
   * @param conditionValue the condition value to compare against
   * @param operator the comparison operator
   * @return true if the condition is met, false otherwise
   */
  private static boolean evaluateSingleCondition(
      @Nonnull String currentValue, @Nonnull String conditionValue, @Nonnull Condition operator) {

    switch (operator) {
      case EQUAL:
        return currentValue.equals(conditionValue);

      case CONTAIN:
        return currentValue.contains(conditionValue);

      case START_WITH:
        return currentValue.startsWith(conditionValue);

      case END_WITH:
        return currentValue.endsWith(conditionValue);

      case GREATER_THAN:
      case GREATER_THAN_OR_EQUAL_TO:
      case LESS_THAN:
      case LESS_THAN_OR_EQUAL_TO:
        // Try numeric comparison for number operators
        try {
          double currentNum = Double.parseDouble(currentValue);
          double conditionNum = Double.parseDouble(conditionValue);
          switch (operator) {
            case GREATER_THAN:
              return currentNum > conditionNum;
            case GREATER_THAN_OR_EQUAL_TO:
              return currentNum >= conditionNum;
            case LESS_THAN:
              return currentNum < conditionNum;
            case LESS_THAN_OR_EQUAL_TO:
              return currentNum <= conditionNum;
          }
        } catch (NumberFormatException e) {
          return false;
        }
        break;

      case EXISTS:
        // For EXISTS, just check if current value is not empty
        return !currentValue.trim().isEmpty();

      default:
        // Default to EQUAL for unknown operators
        return currentValue.equals(conditionValue);
    }

    return false;
  }

  /**
   * Creates or updates an ActionWorkflow.
   *
   * @param opContext the operation context
   * @param workflowUrn the workflow URN, or null to create a new one
   * @param workflowInfo the workflow info
   * @return the URN of the created or updated workflow
   * @throws Exception if the operation fails
   */
  public Urn upsertActionWorkflow(
      @Nonnull OperationContext opContext,
      @Nullable Urn workflowUrn,
      @Nonnull ActionWorkflowInfo workflowInfo)
      throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(workflowInfo, "workflowInfo must not be null");

    // If no URN is provided, generate a new one
    if (workflowUrn == null) {
      workflowUrn =
          UrnUtils.getUrn(String.format("urn:li:actionWorkflow:%s", UUID.randomUUID().toString()));
    }

    // Emit the workflow info
    final MetadataChangeProposal proposal =
        AspectUtils.buildMetadataChangeProposal(
            workflowUrn, ACTION_WORKFLOW_INFO_ASPECT_NAME, workflowInfo);

    _entityClient.ingestProposal(opContext, proposal, false);

    return workflowUrn;
  }

  /**
   * Gets an ActionWorkflow by URN.
   *
   * @param opContext the operation context
   * @param workflowUrn the workflow URN
   * @return the workflow info
   * @throws Exception if the workflow is not found or the operation fails
   */
  public ActionWorkflowInfo getActionWorkflow(
      @Nonnull OperationContext opContext, @Nonnull Urn workflowUrn) throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(workflowUrn, "workflowUrn must not be null");

    final EntityResponse entityResponse =
        this.entityClient.getV2(
            opContext,
            workflowUrn.getEntityType(),
            workflowUrn,
            ImmutableSet.of(ACTION_WORKFLOW_INFO_ASPECT_NAME));

    if (entityResponse == null) {
      throw new RuntimeException(String.format("Failed to find workflow with urn %s", workflowUrn));
    }

    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    final EnvelopedAspect envelopedInfo = aspects.get(ACTION_WORKFLOW_INFO_ASPECT_NAME);
    if (envelopedInfo == null) {
      throw new RuntimeException(
          String.format("Failed to find workflow info with urn %s", workflowUrn));
    }

    return new ActionWorkflowInfo(envelopedInfo.getValue().data());
  }

  /**
   * Deletes an ActionWorkflow by URN.
   *
   * @param opContext the operation context
   * @param workflowUrn the workflow URN to delete
   * @throws Exception if the workflow is not found or the operation fails
   */
  public void deleteActionWorkflow(@Nonnull OperationContext opContext, @Nonnull Urn workflowUrn)
      throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");
    Objects.requireNonNull(workflowUrn, "workflowUrn must not be null");

    // First verify that the workflow exists before attempting to delete
    getActionWorkflow(opContext, workflowUrn);

    // Hard delete the workflow entity
    _entityClient.deleteEntity(opContext, workflowUrn);
  }

  /**
   * Lists ActionWorkflows with optional filtering.
   *
   * @param opContext the operation context
   * @param start the start index
   * @param count the maximum number of results to return
   * @param type the workflow type filter
   * @param customType the custom type filter
   * @param entrypointType the entrypoint type filter
   * @param entityType the entity type filter
   * @return a list of workflows
   * @throws Exception if the operation fails
   */
  public ActionWorkflowListResult listActionWorkflows(
      @Nonnull OperationContext opContext,
      @Nullable Integer start,
      @Nullable Integer count,
      @Nullable ActionWorkflowCategory category,
      @Nullable String customType,
      @Nullable String entrypointType,
      @Nullable String entityType)
      throws Exception {
    Objects.requireNonNull(opContext, "opContext must not be null");

    final int startIndex = start != null ? start : 0;
    final int maxResults = count != null ? count : 20;

    // Build filter criteria
    final List<Criterion> criteria = new ArrayList<>();

    if (category != null) {
      criteria.add(
          new Criterion()
              .setField("category")
              .setCondition(Condition.EQUAL)
              .setValue(category.toString())
              .setValues(new StringArray(Collections.singletonList(category.toString()))));
    }

    if (customType != null) {
      criteria.add(
          new Criterion()
              .setField("customCategory")
              .setCondition(Condition.EQUAL)
              .setValue(customType)
              .setValues(new StringArray(Collections.singletonList(customType))));
    }

    if (entrypointType != null) {
      criteria.add(
          new Criterion()
              .setField("entrypointType")
              .setCondition(Condition.EQUAL)
              .setValue(entrypointType)
              .setValues(new StringArray(Collections.singletonList(entrypointType))));
    }

    if (entityType != null) {
      criteria.add(
          new Criterion()
              .setField("entityTypes")
              .setCondition(Condition.EQUAL)
              .setValue(entityType)
              .setValues(new StringArray(Collections.singletonList(entityType))));
    }

    // Create filter
    Filter filter = null;
    if (!criteria.isEmpty()) {
      filter = new Filter();
      final ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
      conjunctiveCriterion.setAnd(new CriterionArray(criteria));
      filter.setOr(new ConjunctiveCriterionArray(conjunctiveCriterion));
    }

    // Create sort criterion (sort by created time, newest first)
    final SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField("createdAt");
    sortCriterion.setOrder(SortOrder.DESCENDING);

    // Perform search
    final SearchResult searchResult =
        this.entityClient.search(
            opContext,
            ACTION_WORKFLOW_ENTITY_NAME,
            "*",
            filter,
            Collections.singletonList(sortCriterion),
            startIndex,
            maxResults);

    // Convert search results to ActionWorkflowResult objects
    final List<ActionWorkflowResult> workflows = new ArrayList<>();
    for (SearchEntity searchEntity : searchResult.getEntities()) {
      final Urn workflowUrn = searchEntity.getEntity();
      try {
        final ActionWorkflowInfo workflowInfo = getActionWorkflow(opContext, workflowUrn);
        workflows.add(new ActionWorkflowResult(workflowUrn, workflowInfo));
      } catch (Exception e) {
        log.warn("Failed to retrieve workflow info for urn {}", workflowUrn, e);
        // Continue processing other workflows
      }
    }

    return new ActionWorkflowListResult(
        startIndex, workflows.size(), searchResult.getNumEntities(), workflows);
  }

  /**
   * Resolves the actors for a workflow step based on static assignments and dynamic assignment
   * rules.
   *
   * @param entityUrn the entity URN (may be null)
   * @param stepActors the step actors configuration
   * @param opContext the operation context
   * @return resolved actors
   * @throws Exception if resolution fails
   */
  private ActionWorkflowRequestActors resolveActionWorkflowActors(
      @Nullable Urn entityUrn,
      @Nonnull ActionWorkflowStepActors stepActors,
      @Nonnull OperationContext opContext)
      throws Exception {

    List<Urn> users = new ArrayList<>();
    List<Urn> groups = new ArrayList<>();
    List<Urn> roles = new ArrayList<>();

    // Add statically assigned users
    if (stepActors.hasUsers()) {
      users.addAll(stepActors.getUsers());
    }

    // Add statically assigned groups
    if (stepActors.hasGroups()) {
      groups.addAll(stepActors.getGroups());
    }

    // Add statically assigned roles
    if (stepActors.hasRoles()) {
      roles.addAll(stepActors.getRoles());
    }

    // Handle dynamic assignment
    if (stepActors.hasDynamicAssignment()) {
      ActionWorkflowStepDynamicAssignment dynamicAssignment = stepActors.getDynamicAssignment();

      if (entityUrn != null) {
        List<com.linkedin.common.Owner> entityOwners = new ArrayList<>();

        switch (dynamicAssignment.getType()) {
          case ENTITY_OWNERS:
            entityOwners.addAll(
                getEntityOwners(entityUrn, dynamicAssignment.getOwnershipTypeUrns(), opContext));
            break;
          case ENTITY_DOMAIN_OWNERS:
            entityOwners.addAll(
                getEntityDomainOwners(
                    entityUrn, dynamicAssignment.getOwnershipTypeUrns(), opContext));
            break;
          case ENTITY_DATA_PRODUCT_OWNERS:
            entityOwners.addAll(
                getEntityDataProductOwners(
                    entityUrn, dynamicAssignment.getOwnershipTypeUrns(), opContext));
            break;
          default:
            log.warn("Unknown dynamic assignment type: {}", dynamicAssignment.getType());
        }

        // Convert owners to users/groups based on their type
        for (com.linkedin.common.Owner owner : entityOwners) {
          if (owner.getOwner().getEntityType().equals("corpuser")) {
            users.add(owner.getOwner());
          } else if (owner.getOwner().getEntityType().equals("corpGroup")) {
            groups.add(owner.getOwner());
          }
        }
      }
    }

    return new ActionWorkflowRequestActors(users, groups, roles);
  }

  /**
   * Gets the owners of an entity.
   *
   * @param entityUrn the entity URN
   * @param ownershipTypeUrns optional ownership type URNs to filter by
   * @param opContext the operation context
   * @return list of owners
   * @throws Exception if the operation fails
   */
  private List<com.linkedin.common.Owner> getEntityOwners(
      @Nonnull Urn entityUrn,
      @Nullable UrnArray ownershipTypeUrns,
      @Nonnull OperationContext opContext)
      throws Exception {
    List<com.linkedin.common.Owner> allOwners = _ownerService.getEntityOwners(opContext, entityUrn);
    return filterOwnersByType(allOwners, ownershipTypeUrns);
  }

  /**
   * Gets the owners of the domain that the entity belongs to.
   *
   * @param entityUrn the entity URN
   * @param ownershipTypeUrns optional ownership type URNs to filter by
   * @param opContext the operation context
   * @return list of domain owners
   * @throws Exception if the operation fails
   */
  private List<com.linkedin.common.Owner> getEntityDomainOwners(
      @Nonnull Urn entityUrn,
      @Nullable UrnArray ownershipTypeUrns,
      @Nonnull OperationContext opContext)
      throws Exception {
    // First, get the domain of the entity
    final List<Urn> domainUrns = _domainService.getEntityDomains(opContext, entityUrn);
    if (domainUrns == null || domainUrns.isEmpty()) {
      log.warn("No domain found for entity {}", entityUrn);
      return Collections.emptyList();
    }

    // Then, get the owners of the first domain
    final Urn domainUrn = domainUrns.get(0);
    List<com.linkedin.common.Owner> allOwners = _ownerService.getEntityOwners(opContext, domainUrn);
    return filterOwnersByType(allOwners, ownershipTypeUrns);
  }

  /**
   * Gets the owners of the data product that the entity belongs to.
   *
   * @param entityUrn the entity URN
   * @param ownershipTypeUrns optional ownership type URNs to filter by
   * @param opContext the operation context
   * @return list of data product owners
   * @throws Exception if the operation fails
   */
  private List<com.linkedin.common.Owner> getEntityDataProductOwners(
      @Nonnull Urn entityUrn,
      @Nullable UrnArray ownershipTypeUrns,
      @Nonnull OperationContext opContext)
      throws Exception {
    // First, get the data product of the entity
    final List<Urn> dataProductUrns =
        _dataProductService.getDataProductsContainingEntity(opContext, entityUrn);
    if (dataProductUrns.isEmpty()) {
      log.warn("No data product found for entity {}", entityUrn);
      return Collections.emptyList();
    }

    // For now, just use the first data product
    final Urn dataProductUrn = dataProductUrns.get(0);

    // Then, get the owners of the data product
    List<com.linkedin.common.Owner> allOwners =
        _ownerService.getEntityOwners(opContext, dataProductUrn);
    return filterOwnersByType(allOwners, ownershipTypeUrns);
  }

  /**
   * Filters owners by ownership type URNs.
   *
   * @param allOwners the complete list of owners
   * @param ownershipTypeUrns optional ownership type URNs to filter by
   * @return filtered list of owners
   */
  private List<com.linkedin.common.Owner> filterOwnersByType(
      @Nonnull List<com.linkedin.common.Owner> allOwners, @Nullable UrnArray ownershipTypeUrns) {

    // If no ownership type URNs specified, return all owners
    if (ownershipTypeUrns == null || ownershipTypeUrns.isEmpty()) {
      return allOwners;
    }

    // Filter owners by ownership type URN
    return allOwners.stream()
        .filter(
            owner -> {
              // If owner has no typeUrn, exclude it when filtering is active
              if (!owner.hasTypeUrn()) {
                return false;
              }
              // Include owner if its typeUrn matches any of the specified ownership type URNs
              return ownershipTypeUrns.contains(owner.getTypeUrn());
            })
        .collect(java.util.stream.Collectors.toList());
  }

  /** Represents the resolved actors for a workflow request. */
  public static class ActionWorkflowRequestActors {
    private final List<Urn> users;
    private final List<Urn> groups;
    private final List<Urn> roles;

    public ActionWorkflowRequestActors(List<Urn> users, List<Urn> groups, List<Urn> roles) {
      this.users = users != null ? users : new ArrayList<>();
      this.groups = groups != null ? groups : new ArrayList<>();
      this.roles = roles != null ? roles : new ArrayList<>();
    }

    public List<Urn> getUsers() {
      return users;
    }

    public List<Urn> getGroups() {
      return groups;
    }

    public List<Urn> getRoles() {
      return roles;
    }
  }

  /** Represents a workflow result with URN and info. */
  public static class ActionWorkflowResult {
    private final Urn urn;
    private final ActionWorkflowInfo info;

    /**
     * Creates a new ActionWorkflowResult.
     *
     * @param urn the workflow URN
     * @param info the workflow info
     */
    public ActionWorkflowResult(@Nonnull Urn urn, @Nonnull ActionWorkflowInfo info) {
      this.urn = Objects.requireNonNull(urn, "urn must not be null");
      this.info = Objects.requireNonNull(info, "info must not be null");
    }

    /**
     * Gets the workflow URN.
     *
     * @return the URN
     */
    @Nonnull
    public Urn getUrn() {
      return urn;
    }

    /**
     * Gets the workflow info.
     *
     * @return the info
     */
    @Nonnull
    public ActionWorkflowInfo getInfo() {
      return info;
    }
  }

  /** Represents a paginated list of workflow results. */
  public static class ActionWorkflowListResult {
    private final int start;
    private final int count;
    private final int total;
    private final List<ActionWorkflowResult> workflows;

    /**
     * Creates a new ActionWorkflowListResult.
     *
     * @param start the start index
     * @param count the number of results in this page
     * @param total the total number of results
     * @param workflows the workflow results
     */
    public ActionWorkflowListResult(
        int start, int count, int total, @Nonnull List<ActionWorkflowResult> workflows) {
      this.start = start;
      this.count = count;
      this.total = total;
      this.workflows = Objects.requireNonNull(workflows, "workflows must not be null");
    }

    /**
     * Gets the start index.
     *
     * @return the start index
     */
    public int getStart() {
      return start;
    }

    /**
     * Gets the count of results in this page.
     *
     * @return the count
     */
    public int getCount() {
      return count;
    }

    /**
     * Gets the total number of results.
     *
     * @return the total
     */
    public int getTotal() {
      return total;
    }

    /**
     * Gets the workflow results.
     *
     * @return the workflows
     */
    @Nonnull
    public List<ActionWorkflowResult> getWorkflows() {
      return workflows;
    }
  }

  /**
   * Checks if a user is authorized to perform a specific action.
   *
   * @param opContext the operation context
   * @param userUrn the user URN
   * @param assignedUsers the assigned users
   * @param assignedGroups the assigned groups
   * @param assignedRoles the assigned roles
   * @return true if authorized, false otherwise
   * @throws Exception if the operation fails
   */
  private boolean isUserAuthorized(
      @Nonnull OperationContext opContext,
      @Nonnull Urn userUrn,
      @Nullable List<Urn> assignedUsers,
      @Nullable List<Urn> assignedGroups,
      @Nullable List<Urn> assignedRoles)
      throws Exception {

    // Check if user is directly assigned
    if (assignedUsers != null && assignedUsers.contains(userUrn)) {
      return true;
    }

    // Check if user is part of any assigned groups
    if (assignedGroups != null && !assignedGroups.isEmpty()) {
      EntityResponse entityResponse =
          this.entityClient.getV2(
              opContext,
              userUrn.getEntityType(),
              userUrn,
              ImmutableSet.of(GROUP_MEMBERSHIP_ASPECT_NAME, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME));

      if (entityResponse != null) {
        EnvelopedAspectMap aspects = entityResponse.getAspects();

        // Check GROUP_MEMBERSHIP_ASPECT_NAME
        if (aspects.containsKey(GROUP_MEMBERSHIP_ASPECT_NAME)) {
          GroupMembership groupMembership =
              new GroupMembership(aspects.get(GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
          for (Urn groupUrn : groupMembership.getGroups()) {
            if (assignedGroups.contains(groupUrn)) {
              return true;
            }
          }
        }

        // Check NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME
        if (aspects.containsKey(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)) {
          NativeGroupMembership nativeGroupMembership =
              new NativeGroupMembership(
                  aspects.get(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME).getValue().data());
          for (Urn groupUrn : nativeGroupMembership.getNativeGroups()) {
            if (assignedGroups.contains(groupUrn)) {
              return true;
            }
          }
        }
      }
    }

    // Check if user has any assigned roles
    if (assignedRoles != null && !assignedRoles.isEmpty()) {
      EntityResponse entityResponse =
          this.entityClient.getV2(
              opContext,
              userUrn.getEntityType(),
              userUrn,
              ImmutableSet.of(ROLE_MEMBERSHIP_ASPECT_NAME));

      if (entityResponse != null) {
        EnvelopedAspectMap aspects = entityResponse.getAspects();
        if (aspects.containsKey(ROLE_MEMBERSHIP_ASPECT_NAME)) {
          RoleMembership roleMembership =
              new RoleMembership(aspects.get(ROLE_MEMBERSHIP_ASPECT_NAME).getValue().data());
          for (Urn roleUrn : roleMembership.getRoles()) {
            if (assignedRoles.contains(roleUrn)) {
              return true;
            }
          }
        }
      }
    }

    return false;
  }

  /** BELOW THIS LINE IS THE LOGIC TO EXPLORE MOVING INTO BPMN */

  /**
   * Checks if a user is authorized to review a specific action request.
   *
   * @param reviewerUrn the reviewer URN
   * @param actionRequestInfo the action request info
   * @param opContext the operation context
   * @return true if authorized, false otherwise
   * @throws Exception if the operation fails
   */
  private boolean isAuthorizedToReview(
      @Nonnull Urn reviewerUrn,
      @Nonnull ActionRequestInfo actionRequestInfo,
      @Nonnull OperationContext opContext)
      throws Exception {

    return isUserAuthorized(
        opContext,
        reviewerUrn,
        actionRequestInfo.hasAssignedUsers() ? actionRequestInfo.getAssignedUsers() : null,
        actionRequestInfo.hasAssignedGroups() ? actionRequestInfo.getAssignedGroups() : null,
        actionRequestInfo.hasAssignedRoles() ? actionRequestInfo.getAssignedRoles() : null);
  }

  /**
   * Gets the ActionRequestInfo for a given URN.
   *
   * @param opContext the operation context
   * @param actionRequestUrn the action request URN
   * @return the action request info
   * @throws Exception if the operation fails
   */
  public ActionRequestInfo getActionRequestInfo(
      @Nonnull OperationContext opContext, @Nonnull Urn actionRequestUrn) throws Exception {
    final EntityResponse entityResponse =
        this.entityClient.getV2(
            opContext,
            actionRequestUrn.getEntityType(),
            actionRequestUrn,
            ImmutableSet.of(ACTION_REQUEST_INFO_ASPECT_NAME));

    if (entityResponse == null) {
      throw new RuntimeException(
          String.format("Failed to find action request with urn %s", actionRequestUrn));
    }

    final EnvelopedAspectMap aspects = entityResponse.getAspects();
    final EnvelopedAspect envelopedInfo = aspects.get(ACTION_REQUEST_INFO_ASPECT_NAME);
    if (envelopedInfo == null) {
      throw new RuntimeException(
          String.format("Failed to find action request info with urn %s", actionRequestUrn));
    }

    return new ActionRequestInfo(envelopedInfo.getValue().data());
  }

  /**
   * Updates the status of an action request.
   *
   * @param actionRequestUrn the action request URN
   * @param actorUrn the actor URN
   * @param status the new status
   * @param result the result
   * @param note optional note
   * @param opContext the operation context
   * @throws Exception if the operation fails
   */
  private void updateActionRequestStatus(
      @Nonnull Urn actionRequestUrn,
      @Nonnull Urn actorUrn,
      @Nonnull String status,
      @Nonnull String result,
      @Nullable String note,
      @Nonnull OperationContext opContext)
      throws Exception {

    // Create the ActionRequestStatus
    final ActionRequestStatus actionRequestStatus = new ActionRequestStatus();
    actionRequestStatus.setStatus(status);
    actionRequestStatus.setResult(result, SetMode.IGNORE_NULL);
    actionRequestStatus.setNote(note, SetMode.IGNORE_NULL);
    actionRequestStatus.setLastModified(
        new AuditStamp().setActor(actorUrn).setTime(System.currentTimeMillis()));

    // Build and emit the ActionRequestStatus proposal
    final MetadataChangeProposal proposal =
        AspectUtils.buildMetadataChangeProposal(
            actionRequestUrn, ACTION_REQUEST_STATUS_ASPECT_NAME, actionRequestStatus);

    _entityClient.ingestProposal(opContext, proposal, false);
  }

  /**
   * Updates the workflow step or completes the workflow for a form triggered request. Note that
   * form requests only support REQUEST_REVIEW step type as of today. We implement the state
   * transition logic ourselves when review steps are completed, for now.
   *
   * @param actionRequestUrn the action request URN
   * @param actorUrn the actor URN
   * @param workflowInfo the workflow info
   * @param currentStepId the current step ID
   * @param comment optional comment
   * @param opContext the operation context
   * @throws Exception if the operation fails
   */
  private void updateFormRequestStepOrComplete(
      @Nonnull Urn actionRequestUrn,
      @Nonnull Urn actorUrn,
      @Nonnull ActionWorkflowInfo workflowInfo,
      @Nonnull String currentStepId,
      @Nullable String comment,
      @Nonnull OperationContext opContext)
      throws Exception {

    // Get the current ActionRequestInfo
    ActionRequestInfo actionRequestInfo = getActionRequestInfo(opContext, actionRequestUrn);

    // Find the current step index
    List<ActionWorkflowStep> steps = workflowInfo.getSteps();
    int currentStepIndex = -1;
    for (int i = 0; i < steps.size(); i++) {
      if (steps.get(i).getId().equals(currentStepId)) {
        currentStepIndex = i;
        break;
      }
    }

    if (currentStepIndex == -1) {
      throw new IllegalArgumentException("Current step not found in workflow: " + currentStepId);
    }

    // Check if this is the last step
    if (currentStepIndex == steps.size() - 1) {
      // This is the last step, complete the workflow
      updateActionRequestStatus(
          actionRequestUrn,
          actorUrn,
          ACTION_REQUEST_STATUS_COMPLETE,
          ACTION_REQUEST_RESULT_ACCEPTED,
          comment,
          opContext);
    } else {
      // Move to the next step
      ActionWorkflowStep nextStep = steps.get(currentStepIndex + 1);

      // Resolve the actors for the next step -- currently the only step type supported it
      // REQUEST_REVIEW
      // for form-triggered workflows.
      if (!nextStep.getType().equals(ActionWorkflowStepType.APPROVAL)) {
        throw new IllegalArgumentException(
            "Only REQUEST_REVIEW step type is currently supported for form-triggered workflows");
      }
      ActionWorkflowRequestActors nextStepActors =
          resolveActionWorkflowActors(
              actionRequestInfo.hasResource()
                  ? UrnUtils.getUrn(actionRequestInfo.getResource())
                  : null,
              nextStep.getActors(),
              opContext);

      // Update the action request with the new step and actors
      actionRequestInfo.setAssignedUsers(new UrnArray(nextStepActors.getUsers()));
      actionRequestInfo.setAssignedGroups(new UrnArray(nextStepActors.getGroups()));
      actionRequestInfo.setAssignedRoles(new UrnArray(nextStepActors.getRoles()));

      // Update the step state in the workflow request
      if (actionRequestInfo.hasParams() && actionRequestInfo.getParams().hasWorkflowFormRequest()) {
        ActionWorkflowFormRequest workflowRequest =
            actionRequestInfo.getParams().getWorkflowFormRequest();
        ActionWorkflowRequestStepState stepState = workflowRequest.getStepState();
        stepState.setStepId(nextStep.getId());
      }

      // Save the updated ActionRequestInfo
      final MetadataChangeProposal proposal =
          AspectUtils.buildMetadataChangeProposal(
              actionRequestUrn, ACTION_REQUEST_INFO_ASPECT_NAME, actionRequestInfo);

      _entityClient.ingestProposal(opContext, proposal, false);
    }
  }

  /**
   * Reviews an action request workflow form request.
   *
   * @param actionRequestUrn the action request URN
   * @param reviewerUrn the reviewer URN
   * @param result the review result
   * @param comment optional comment
   * @param opContext the operation context
   * @return true if the review was successful, false otherwise
   * @throws Exception if the operation fails
   */
  public boolean reviewActionWorkflowFormRequest(
      @Nonnull Urn actionRequestUrn,
      @Nonnull Urn reviewerUrn,
      @Nonnull String result,
      @Nullable String comment,
      @Nonnull OperationContext opContext)
      throws Exception {

    Objects.requireNonNull(actionRequestUrn, "actionRequestUrn must not be null");
    Objects.requireNonNull(reviewerUrn, "reviewerUrn must not be null");
    Objects.requireNonNull(result, "result must not be null");
    Objects.requireNonNull(opContext, "opContext must not be null");

    // Get the ActionRequestInfo
    ActionRequestInfo actionRequestInfo = getActionRequestInfo(opContext, actionRequestUrn);

    // Check if the user is authorized to review this request
    if (!isAuthorizedToReview(reviewerUrn, actionRequestInfo, opContext)) {
      throw new RuntimeException(
          String.format(
              "User %s is not authorized to review action request %s",
              reviewerUrn, actionRequestUrn));
    }

    // Handle the review result
    if (ACTION_REQUEST_RESULT_REJECTED.equals(result)) {
      // If rejected, complete the request as rejected
      updateActionRequestStatus(
          actionRequestUrn,
          reviewerUrn,
          ACTION_REQUEST_STATUS_COMPLETE,
          ACTION_REQUEST_RESULT_REJECTED,
          comment,
          opContext);
    } else if (ACTION_REQUEST_RESULT_ACCEPTED.equals(result)) {
      // If accepted, either move to next step or complete
      if (actionRequestInfo.hasParams() && actionRequestInfo.getParams().hasWorkflowFormRequest()) {
        ActionWorkflowFormRequest workflowRequest =
            actionRequestInfo.getParams().getWorkflowFormRequest();
        ActionWorkflowInfo workflowInfo =
            getActionWorkflow(opContext, workflowRequest.getWorkflow());

        String currentStepId = workflowRequest.getStepState().getStepId();
        updateFormRequestStepOrComplete(
            actionRequestUrn, reviewerUrn, workflowInfo, currentStepId, comment, opContext);
      } else {
        // No workflow request found, just complete as accepted
        updateActionRequestStatus(
            actionRequestUrn,
            reviewerUrn,
            ACTION_REQUEST_STATUS_COMPLETE,
            ACTION_REQUEST_RESULT_ACCEPTED,
            comment,
            opContext);
      }
    } else {
      throw new IllegalArgumentException("Invalid result: " + result);
    }

    return true;
  }
}
