package com.linkedin.datahub.graphql.resolvers.actionworkflows;

import com.linkedin.actionworkflow.ActionWorkflowEntrypointArray;
import com.linkedin.actionworkflow.ActionWorkflowField;
import com.linkedin.actionworkflow.ActionWorkflowFieldArray;
import com.linkedin.actionworkflow.ActionWorkflowInfo;
import com.linkedin.actionworkflow.ActionWorkflowStepArray;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.*;
import com.linkedin.datahub.graphql.types.entitytype.EntityTypeUrnMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Mapper class to convert ActionWorkflowInfo (PDL) to ActionWorkflow (GraphQL) */
public class ActionWorkflowMapper {

  /** Maps ActionWorkflowInfo to ActionWorkflow */
  public static ActionWorkflow map(
      @Nonnull Urn workflowUrn, @Nonnull ActionWorkflowInfo workflowInfo) {

    final ActionWorkflow workflow = new ActionWorkflow();
    workflow.setUrn(workflowUrn.toString());
    workflow.setName(workflowInfo.getName());
    workflow.setCategory(ActionWorkflowCategory.valueOf(workflowInfo.getCategory().toString()));
    workflow.setCustomCategory(workflowInfo.getCustomCategory());

    workflow.setDescription(workflowInfo.getDescription());

    // Map trigger information
    workflow.setTrigger(mapWorkflowTrigger(workflowInfo.getTrigger()));

    // Map steps
    workflow.setSteps(mapWorkflowSteps(workflowInfo.getSteps()));

    // Map audit information
    workflow.setCreated(mapAuditStamp(workflowInfo.getCreated()));
    workflow.setLastModified(mapAuditStamp(workflowInfo.getLastModified()));

    return workflow;
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowTrigger mapWorkflowTrigger(
      @Nonnull final com.linkedin.actionworkflow.ActionWorkflowTrigger trigger) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowTrigger graphQLTrigger =
        new com.linkedin.datahub.graphql.generated.ActionWorkflowTrigger();

    graphQLTrigger.setType(ActionWorkflowTriggerType.valueOf(trigger.getType().toString()));

    // Map form if present - extract its contents to the trigger level
    if (trigger.hasForm()) {
      final com.linkedin.datahub.graphql.generated.ActionWorkflowForm graphQLForm =
          new com.linkedin.datahub.graphql.generated.ActionWorkflowForm();
      com.linkedin.actionworkflow.ActionWorkflowForm form = trigger.getForm();

      // Map entity types
      if (form.hasEntityTypes()) {
        List<EntityType> entityTypes =
            form.getEntityTypes().stream()
                .map(entityTypeUrn -> EntityTypeUrnMapper.getEntityType(entityTypeUrn.toString()))
                .collect(Collectors.toList());
        graphQLForm.setEntityTypes(entityTypes);
      }

      // Map fields
      if (form.hasFields()) {
        graphQLForm.setFields(mapWorkflowFields(form.getFields()));
      } else {
        graphQLForm.setFields(Collections.emptyList());
      }

      // Map entrypoints
      if (form.hasEntrypoints()) {
        graphQLForm.setEntrypoints(mapWorkflowEntrypoints(form.getEntrypoints()));
      } else {
        graphQLForm.setEntrypoints(Collections.emptyList());
      }

      graphQLTrigger.setForm(graphQLForm);
    }

    return graphQLTrigger;
  }

  private static List<com.linkedin.datahub.graphql.generated.ActionWorkflowField> mapWorkflowFields(
      @Nonnull final ActionWorkflowFieldArray fields) {
    return fields.stream().map(ActionWorkflowMapper::mapWorkflowField).collect(Collectors.toList());
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowField mapWorkflowField(
      @Nonnull final ActionWorkflowField field) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowField graphQLField =
        new com.linkedin.datahub.graphql.generated.ActionWorkflowField();
    graphQLField.setId(field.getId());
    graphQLField.setName(field.getName());
    graphQLField.setDescription(field.getDescription());
    graphQLField.setValueType(mapUrnToActionWorkflowFieldValueType(field.getValueType()));

    if (field.hasAllowedEntityTypes()) {
      graphQLField.setAllowedEntityTypes(
          field.getAllowedEntityTypes().stream()
              .map(entityTypeUrn -> EntityTypeUrnMapper.getEntityType(entityTypeUrn.toString()))
              .collect(Collectors.toList()));
    }

    // Map allowed values
    if (field.hasAllowedValues()) {
      List<PropertyValue> allowedValues =
          field.getAllowedValues().stream()
              .map(ActionWorkflowMapper::mapToGraphQLPropertyValue)
              .collect(Collectors.toList());
      graphQLField.setAllowedValues(allowedValues);
    }

    graphQLField.setCardinality(PropertyCardinality.valueOf(field.getCardinality().toString()));
    graphQLField.setRequired(field.isRequired());

    // Map condition field if present
    if (field.hasCondition()) {
      graphQLField.setCondition(mapWorkflowFieldCondition(field.getCondition()));
    }

    return graphQLField;
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowFieldCondition
      mapWorkflowFieldCondition(
          @Nonnull final com.linkedin.actionworkflow.ActionWorkflowFieldCondition condition) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowFieldCondition graphQLCondition =
        new com.linkedin.datahub.graphql.generated.ActionWorkflowFieldCondition();

    graphQLCondition.setType(
        ActionWorkflowFieldConditionType.valueOf(condition.getType().toString()));

    if (condition.hasSingleFieldValueCondition()) {
      graphQLCondition.setSingleFieldValueCondition(
          mapSingleFieldValueCondition(condition.getSingleFieldValueCondition()));
    }

    return graphQLCondition;
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowSingleFieldValueCondition
      mapSingleFieldValueCondition(
          @Nonnull
              final com.linkedin.actionworkflow.ActionWorkflowSingleFieldValueCondition
                  singleCondition) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowSingleFieldValueCondition
        graphQLSingleCondition =
            new com.linkedin.datahub.graphql.generated.ActionWorkflowSingleFieldValueCondition();

    graphQLSingleCondition.setField(singleCondition.getField());
    graphQLSingleCondition.setValues(new ArrayList<>(singleCondition.getValues()));
    graphQLSingleCondition.setCondition(
        FilterOperator.valueOf(singleCondition.getCondition().toString()));
    graphQLSingleCondition.setNegated(singleCondition.isNegated());

    return graphQLSingleCondition;
  }

  private static ActionWorkflowFieldValueType mapUrnToActionWorkflowFieldValueType(
      Urn valueTypeUrn) {
    String urnString = valueTypeUrn.toString();
    if (urnString.contains("datahub.string")) {
      return ActionWorkflowFieldValueType.STRING;
    } else if (urnString.contains("datahub.rich_text")) {
      return ActionWorkflowFieldValueType.RICH_TEXT;
    } else if (urnString.contains("datahub.urn")) {
      return ActionWorkflowFieldValueType.URN;
    } else if (urnString.contains("datahub.date")) {
      return ActionWorkflowFieldValueType.DATE;
    } else if (urnString.contains("datahub.number")) {
      return ActionWorkflowFieldValueType.NUMBER;
    } else {
      return ActionWorkflowFieldValueType.STRING; // Default fallback
    }
  }

  private static PropertyValue mapToGraphQLPropertyValue(
      @Nonnull final com.linkedin.structured.PropertyValue propertyValue) {
    if (propertyValue.getValue().isString()) {
      StringValue stringValue = new StringValue();
      stringValue.setStringValue(propertyValue.getValue().getString());
      return stringValue;
    } else if (propertyValue.getValue().isDouble()) {
      NumberValue numberValue = new NumberValue();
      numberValue.setNumberValue(propertyValue.getValue().getDouble().floatValue());
      return numberValue;
    }
    return null;
  }

  private static List<com.linkedin.datahub.graphql.generated.ActionWorkflowEntrypoint>
      mapWorkflowEntrypoints(@Nonnull final ActionWorkflowEntrypointArray entrypoints) {
    return entrypoints.stream()
        .map(ActionWorkflowMapper::mapWorkflowEntrypoint)
        .collect(Collectors.toList());
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowEntrypoint
      mapWorkflowEntrypoint(
          @Nonnull final com.linkedin.actionworkflow.ActionWorkflowEntrypoint entrypoint) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowEntrypoint graphQLEntrypoint =
        new com.linkedin.datahub.graphql.generated.ActionWorkflowEntrypoint();
    graphQLEntrypoint.setType(
        ActionWorkflowEntrypointType.valueOf(entrypoint.getType().toString()));
    graphQLEntrypoint.setLabel(entrypoint.getLabel());
    return graphQLEntrypoint;
  }

  private static List<com.linkedin.datahub.graphql.generated.ActionWorkflowStep> mapWorkflowSteps(
      @Nonnull final ActionWorkflowStepArray steps) {
    return steps.stream().map(ActionWorkflowMapper::mapWorkflowStep).collect(Collectors.toList());
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowStep mapWorkflowStep(
      @Nonnull final com.linkedin.actionworkflow.ActionWorkflowStep step) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowStep graphQLStep =
        new com.linkedin.datahub.graphql.generated.ActionWorkflowStep();
    graphQLStep.setId(step.getId());
    graphQLStep.setType(ActionWorkflowStepType.valueOf(step.getType().toString()));
    graphQLStep.setDescription(step.getDescription());
    graphQLStep.setActors(mapWorkflowStepActors(step.getActors()));
    return graphQLStep;
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowStepActors
      mapWorkflowStepActors(
          @Nonnull final com.linkedin.actionworkflow.ActionWorkflowStepActors actors) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowStepActors graphQLActors =
        new com.linkedin.datahub.graphql.generated.ActionWorkflowStepActors();

    // Map users
    if (actors.hasUsers()) {
      List<CorpUser> users =
          actors.getUsers().stream()
              .map(
                  urn -> {
                    CorpUser user = new CorpUser();
                    user.setUrn(urn.toString());
                    return user;
                  })
              .collect(Collectors.toList());
      graphQLActors.setUsers(users);
    }

    // Map groups
    if (actors.hasGroups()) {
      List<CorpGroup> groups =
          actors.getGroups().stream()
              .map(
                  urn -> {
                    CorpGroup group = new CorpGroup();
                    group.setUrn(urn.toString());
                    return group;
                  })
              .collect(Collectors.toList());
      graphQLActors.setGroups(groups);
    }

    // Map roles
    if (actors.hasRoles()) {
      List<DataHubRole> roles =
          actors.getRoles().stream()
              .map(
                  urn -> {
                    DataHubRole role = new DataHubRole();
                    role.setUrn(urn.toString());
                    return role;
                  })
              .collect(Collectors.toList());
      graphQLActors.setRoles(roles);
    }

    // Map dynamic assignment
    if (actors.hasDynamicAssignment()) {
      graphQLActors.setDynamicAssignment(
          mapWorkflowStepDynamicAssignment(actors.getDynamicAssignment()));
    }

    return graphQLActors;
  }

  private static com.linkedin.datahub.graphql.generated.ActionWorkflowStepDynamicAssignment
      mapWorkflowStepDynamicAssignment(
          @Nonnull
              final com.linkedin.actionworkflow.ActionWorkflowStepDynamicAssignment
                  dynamicAssignment) {
    final com.linkedin.datahub.graphql.generated.ActionWorkflowStepDynamicAssignment
        graphQLDynamicAssignment =
            new com.linkedin.datahub.graphql.generated.ActionWorkflowStepDynamicAssignment();

    graphQLDynamicAssignment.setType(
        ActionWorkflowStepDynamicAssignmentType.valueOf(dynamicAssignment.getType().toString()));

    if (dynamicAssignment.hasOwnershipTypeUrns()) {
      List<OwnershipTypeEntity> ownershipTypes =
          dynamicAssignment.getOwnershipTypeUrns().stream()
              .map(
                  urn -> {
                    OwnershipTypeEntity ownershipType = new OwnershipTypeEntity();
                    ownershipType.setUrn(urn.toString());
                    return ownershipType;
                  })
              .collect(Collectors.toList());
      graphQLDynamicAssignment.setOwnershipTypes(ownershipTypes);
    }

    return graphQLDynamicAssignment;
  }

  private static ResolvedAuditStamp mapAuditStamp(@Nonnull AuditStamp auditStamp) {
    ResolvedAuditStamp resolvedAuditStamp = new ResolvedAuditStamp();
    resolvedAuditStamp.setTime(auditStamp.getTime());

    // Create CorpUser object for the actor
    CorpUser actor = new CorpUser();
    actor.setUrn(auditStamp.getActor().toString());
    resolvedAuditStamp.setActor(actor);

    return resolvedAuditStamp;
  }
}
