package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.actionworkflow.ActionWorkflowCategory;
import com.linkedin.actionworkflow.ActionWorkflowEntrypointArray;
import com.linkedin.actionworkflow.ActionWorkflowFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowForm;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.actionworkflow.ActionWorkflowStepArray;
import com.linkedin.actionworkflow.ActionWorkflowTrigger;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.resolvers.actionrequest.ActionRequestUtils;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeMapper;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeUrnMapper;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.service.ActionWorkflowService;
import com.linkedin.structured.PropertyValueArray;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpsertActionWorkflowResolver
    implements DataFetcher<CompletableFuture<ActionWorkflow>> {

  private final ActionWorkflowService _actionWorkflowService;

  @Override
  public CompletableFuture<ActionWorkflow> get(DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpsertActionWorkflowInput input =
        bindArgument(environment.getArgument("input"), UpsertActionWorkflowInput.class);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            return upsertActionWorkflow(input, context);
          } catch (Exception e) {
            log.error("Failed to upsert action workflow", e);
            throw new RuntimeException("Failed to upsert action workflow", e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private ActionWorkflow upsertActionWorkflow(
      @Nonnull final UpsertActionWorkflowInput input, @Nonnull final QueryContext context)
      throws Exception {

    // Check authorization - user must have MANAGE_ACTION_WORKFLOWS privilege
    if (!ActionRequestUtils.canManageActionWorkflows(context)) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }

    final Urn actorUrn = UrnUtils.getUrn(context.getActorUrn());
    Urn workflowUrn = null;
    if (input.getUrn() != null) {
      workflowUrn = UrnUtils.getUrn(input.getUrn());
    }

    // Create the PDL workflow info object
    final ActionWorkflowInfo workflowInfo = new ActionWorkflowInfo();
    workflowInfo.setName(input.getName());
    workflowInfo.setCategory(ActionWorkflowCategory.valueOf(input.getCategory().toString()));
    workflowInfo.setCustomCategory(input.getCustomCategory(), SetMode.IGNORE_NULL);

    final ActionWorkflowTrigger workflowTrigger = new ActionWorkflowTrigger();
    workflowTrigger.setType(
        com.linkedin.actionworkflow.ActionWorkflowTriggerType.valueOf(
            input.getTrigger().getType().toString()));

    if (input.getTrigger().getForm() != null) {

      final ActionWorkflowForm workflowForm = new ActionWorkflowForm();

      ActionWorkflowFormInput formInput = input.getTrigger().getForm();
      // Convert entity type strings to URNs
      if (formInput.getEntityTypes() != null && !formInput.getEntityTypes().isEmpty()) {
        List<Urn> entityTypeUrns = new ArrayList<>();
        for (EntityType entityType : formInput.getEntityTypes()) {
          try {
            // Try to parse as URN first
            entityTypeUrns.add(
                UrnUtils.getUrn(
                    EntityTypeUrnMapper.getEntityTypeUrn(EntityTypeMapper.getName(entityType))));
          } catch (Exception e) {
            // If not a URN, assume it's a simple entity type name
            entityTypeUrns.add(UrnUtils.getUrn("urn:li:entityType:datahub." + entityType));
          }
        }
        workflowForm.setEntityTypes(new UrnArray(entityTypeUrns));
      }
      workflowForm.setFields(createWorkflowFields(formInput.getFields()));
      workflowForm.setEntrypoints(createWorkflowEntrypoints(formInput.getEntrypoints()));
      workflowTrigger.setForm(workflowForm);
    }

    workflowInfo.setTrigger(workflowTrigger);

    workflowInfo.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    workflowInfo.setSteps(createWorkflowSteps(input.getSteps()));

    // Set audit information
    final AuditStamp createdStamp = new AuditStamp();
    createdStamp.setActor(actorUrn);
    createdStamp.setTime(System.currentTimeMillis());
    workflowInfo.setCreated(createdStamp);

    final AuditStamp lastModifiedStamp = new AuditStamp();
    lastModifiedStamp.setActor(actorUrn);
    lastModifiedStamp.setTime(System.currentTimeMillis());
    workflowInfo.setLastModified(lastModifiedStamp);

    // Use the service to handle the upsert operation
    final Urn finalWorkflowUrn =
        _actionWorkflowService.upsertActionWorkflow(
            context.getOperationContext(), workflowUrn, workflowInfo);

    // Get the workflow info and map it to GraphQL response
    final ActionWorkflowInfo retrievedWorkflowInfo =
        _actionWorkflowService.getActionWorkflow(context.getOperationContext(), finalWorkflowUrn);

    return ActionWorkflowMapper.map(finalWorkflowUrn, retrievedWorkflowInfo);
  }

  private ActionWorkflowFieldArray createWorkflowFields(List<ActionWorkflowFieldInput> fields) {
    if (fields == null) {
      return new ActionWorkflowFieldArray();
    }
    List<com.linkedin.actionworkflow.ActionWorkflowField> pdlFields =
        fields.stream().map(this::createWorkflowField).collect(Collectors.toList());
    return new ActionWorkflowFieldArray(pdlFields);
  }

  private com.linkedin.actionworkflow.ActionWorkflowField createWorkflowField(
      ActionWorkflowFieldInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowField field =
        new com.linkedin.actionworkflow.ActionWorkflowField();
    field.setId(input.getId());
    field.setName(input.getName());
    field.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    // Map value type from GraphQL enum to URN
    field.setValueType(mapValueTypeToUrn(input.getValueType()));
    // Map allowed values
    if (input.getAllowedValues() != null && !input.getAllowedValues().isEmpty()) {
      List<com.linkedin.structured.PropertyValue> allowedValues =
          input.getAllowedValues().stream()
              .map(this::createPropertyValue)
              .filter(java.util.Objects::nonNull)
              .collect(Collectors.toList());
      field.setAllowedValues(new PropertyValueArray(allowedValues));
    }
    if (input.getAllowedEntityTypes() != null && !input.getAllowedEntityTypes().isEmpty()) {
      List<Urn> allowedEntityTypeUrns =
          input.getAllowedEntityTypes().stream()
              .map(
                  entityType ->
                      UrnUtils.getUrn(
                          EntityTypeUrnMapper.getEntityTypeUrn(
                              EntityTypeMapper.getName(entityType))))
              .collect(Collectors.toList());
      field.setAllowedEntityTypes(new UrnArray(allowedEntityTypeUrns));
    }
    field.setCardinality(
        com.linkedin.structured.PropertyCardinality.valueOf(input.getCardinality().toString()));
    field.setRequired(input.getRequired() != null ? input.getRequired() : false);

    // Map condition field if present
    if (input.getCondition() != null) {
      field.setCondition(createWorkflowFieldCondition(input.getCondition()));
    }

    return field;
  }

  private com.linkedin.actionworkflow.ActionWorkflowFieldCondition createWorkflowFieldCondition(
      ActionWorkflowFieldConditionInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition =
        new com.linkedin.actionworkflow.ActionWorkflowFieldCondition();

    condition.setType(
        com.linkedin.actionworkflow.ActionWorkflowFieldConditionType.valueOf(
            input.getType().toString()));

    if (input.getSingleFieldValueCondition() != null) {
      condition.setSingleFieldValueCondition(
          createSingleFieldValueCondition(input.getSingleFieldValueCondition()));
    }

    return condition;
  }

  private com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition
      createSingleFieldValueCondition(ActionWorkflowSingleFieldValueConditionInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition singleCondition =
        new com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition();

    singleCondition.setField(input.getField());
    singleCondition.setValues(new StringArray(input.getValues()));
    singleCondition.setCondition(Condition.valueOf(input.getCondition().toString()));
    singleCondition.setNegated(input.getNegated() != null ? input.getNegated() : false);

    return singleCondition;
  }

  private Urn mapValueTypeToUrn(ActionWorkflowFieldValueType valueType) {
    switch (valueType) {
      case STRING:
        return UrnUtils.getUrn("urn:li:dataType:datahub.string");
      case RICH_TEXT:
        return UrnUtils.getUrn("urn:li:dataType:datahub.rich_text");
      case URN:
        return UrnUtils.getUrn("urn:li:dataType:datahub.urn");
      case DATE:
        return UrnUtils.getUrn("urn:li:dataType:datahub.date");
      case NUMBER:
        return UrnUtils.getUrn("urn:li:dataType:datahub.number");
      default:
        throw new IllegalArgumentException("Unknown value type: " + valueType);
    }
  }

  private com.linkedin.structured.PropertyValue createPropertyValue(PropertyValueInput input) {
    com.linkedin.structured.PropertyValue propertyValue =
        new com.linkedin.structured.PropertyValue();
    // Create property value based on input type
    if (input.getStringValue() != null) {
      propertyValue.setValue(
          com.linkedin.structured.PrimitivePropertyValue.create(input.getStringValue()));
    } else if (input.getNumberValue() != null) {
      propertyValue.setValue(
          com.linkedin.structured.PrimitivePropertyValue.create(
              input.getNumberValue().doubleValue()));
    }
    return propertyValue;
  }

  private UrnArray toUrnArray(List<Urn> urns) {
    return new UrnArray(urns);
  }

  private ActionWorkflowEntrypointArray createWorkflowEntrypoints(
      List<ActionWorkflowEntrypointInput> entrypoints) {
    if (entrypoints == null) {
      return new ActionWorkflowEntrypointArray();
    }
    List<com.linkedin.actionworkflow.ActionWorkflowEntrypoint> pdlEntrypoints =
        entrypoints.stream().map(this::createWorkflowEntrypoint).collect(Collectors.toList());
    return new ActionWorkflowEntrypointArray(pdlEntrypoints);
  }

  private com.linkedin.actionworkflow.ActionWorkflowEntrypoint createWorkflowEntrypoint(
      ActionWorkflowEntrypointInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowEntrypoint entrypoint =
        new com.linkedin.actionworkflow.ActionWorkflowEntrypoint();
    entrypoint.setType(
        com.linkedin.actionworkflow.ActionWorkflowEntrypointType.valueOf(
            input.getType().toString()));
    entrypoint.setLabel(input.getLabel());
    return entrypoint;
  }

  private ActionWorkflowStepArray createWorkflowSteps(List<ActionWorkflowStepInput> steps) {
    if (steps == null) {
      return new ActionWorkflowStepArray();
    }
    List<com.linkedin.actionworkflow.ActionWorkflowStep> pdlSteps =
        steps.stream().map(this::createWorkflowStep).collect(Collectors.toList());
    return new ActionWorkflowStepArray(pdlSteps);
  }

  private com.linkedin.actionworkflow.ActionWorkflowStep createWorkflowStep(
      ActionWorkflowStepInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowStep step =
        new com.linkedin.actionworkflow.ActionWorkflowStep();
    step.setId(input.getId());
    step.setDescription(input.getDescription(), SetMode.IGNORE_NULL);
    step.setActors(createWorkflowStepActors(input.getActors()));
    step.setType(
        com.linkedin.actionworkflow.ActionWorkflowStepType.valueOf(input.getType().toString()));
    return step;
  }

  private com.linkedin.actionworkflow.ActionWorkflowStepActors createWorkflowStepActors(
      ActionWorkflowStepActorsInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowStepActors actors =
        new com.linkedin.actionworkflow.ActionWorkflowStepActors();

    actors.setUsers(
        new UrnArray(
            input.getUserUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList())));

    actors.setGroups(
        new UrnArray(
            input.getGroupUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList())));

    actors.setRoles(
        new UrnArray(
            input.getRoleUrns().stream().map(UrnUtils::getUrn).collect(Collectors.toList())));

    if (input.getDynamicAssignment() != null) {
      actors.setDynamicAssignment(createDynamicAssignment(input.getDynamicAssignment()));
    }

    return actors;
  }

  private com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment createDynamicAssignment(
      ActionWorkflowStepDynamicAssignmentInput input) {
    final com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment dynamicAssignment =
        new com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment();

    dynamicAssignment.setType(
        com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignmentType.valueOf(
            input.getType().toString()));

    if (input.getOwnershipTypeUrns() != null && !input.getOwnershipTypeUrns().isEmpty()) {
      dynamicAssignment.setOwnershipTypeUrns(
          new UrnArray(
              input.getOwnershipTypeUrns().stream()
                  .map(UrnUtils::getUrn)
                  .collect(Collectors.toList())));
    }

    return dynamicAssignment;
  }
}
