package com.linkedin.metadata.timeline.eventgenerator;

import static com.linkedin.metadata.AcrylConstants.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.actionrequest.*;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.timeline.data.ChangeCategory;
import com.linkedin.metadata.timeline.data.ChangeEvent;
import com.linkedin.metadata.timeline.data.ChangeOperation;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import java.util.*;
import javax.annotation.Nonnull;

public class ActionRequestInfoChangeEventGenerator
    extends EntityChangeEventGenerator<ActionRequestInfo> {
  @Override
  public List<ChangeEvent> getChangeEvents(
      @Nonnull Urn urn,
      @Nonnull String entity,
      @Nonnull String aspect,
      @Nonnull Aspect<ActionRequestInfo> from,
      @Nonnull Aspect<ActionRequestInfo> to,
      @Nonnull AuditStamp auditStamp) {
    return computeDiffs(from.getValue(), to.getValue(), urn.toString(), auditStamp);
  }

  private List<ChangeEvent> computeDiffs(
      final ActionRequestInfo previousAspect,
      final ActionRequestInfo newAspect,
      @Nonnull final String entityUrn,
      @Nonnull final AuditStamp auditStamp) {
    if (newAspect == null) {
      return Collections.emptyList();
    }
    if (previousAspect != null && previousAspect.equals(newAspect)) {
      return Collections.emptyList();
    }

    return Collections.singletonList(
        ChangeEvent.builder()
            .category(ChangeCategory.LIFECYCLE)
            .operation(ChangeOperation.CREATE)
            .auditStamp(auditStamp)
            .entityUrn(entityUrn)
            .parameters(buildParameters(newAspect))
            .build());
  }

  @Nonnull
  private Map<String, Object> buildParameters(@Nonnull final ActionRequestInfo actionRequestInfo) {
    if (!actionRequestInfo.hasParams()) {
      return Collections.emptyMap();
    }
    Map<String, Object> parameters = new HashMap<>();

    parameters.put(ACTION_REQUEST_TYPE_KEY, actionRequestInfo.getType());

    if (actionRequestInfo.hasResourceType()) {
      parameters.put(RESOURCE_TYPE_KEY, actionRequestInfo.getResourceType());
    }

    if (actionRequestInfo.hasResource()) {
      parameters.put(RESOURCE_URN_KEY, actionRequestInfo.getResource());
    }

    if (actionRequestInfo.hasSubResourceType()) {
      parameters.put(SUB_RESOURCE_TYPE_KEY, actionRequestInfo.getSubResourceType());
    }

    if (actionRequestInfo.hasSubResource()) {
      parameters.put(SUB_RESOURCE_KEY, actionRequestInfo.getSubResource());
    }

    if (actionRequestInfo.hasParams()) {
      final ActionRequestParams actionRequestParams = actionRequestInfo.getParams();
      Map<String, Object> extendedParameters =
          buildActionRequestParamsParameters(actionRequestParams);
      parameters.putAll(extendedParameters);
    }

    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildActionRequestParamsParameters(
      @Nonnull final ActionRequestParams actionRequestParams) {
    if (actionRequestParams.hasGlossaryTermProposal()) {
      return buildGlossaryTermProposalParameters(actionRequestParams.getGlossaryTermProposal());
    }

    if (actionRequestParams.hasTagProposal()) {
      return buildTagProposalParameters(actionRequestParams.getTagProposal());
    }

    if (actionRequestParams.hasCreateGlossaryNodeProposal()) {
      return buildCreateGlossaryNodeProposalParameters(
          actionRequestParams.getCreateGlossaryNodeProposal());
    }

    if (actionRequestParams.hasCreateGlossaryTermProposal()) {
      return buildCreateGlossaryTermProposalParameters(
          actionRequestParams.getCreateGlossaryTermProposal());
    }

    if (actionRequestParams.hasUpdateDescriptionProposal()) {
      return buildUpdateDescriptionProposalParameters(
          actionRequestParams.getUpdateDescriptionProposal());
    }

    if (actionRequestParams.hasStructuredPropertyProposal()) {
      return buildStructuredPropertyProposalParameters(
          actionRequestParams.getStructuredPropertyProposal());
    }

    if (actionRequestParams.hasOwnerProposal()) {
      return buildOwnerProposalParameters(actionRequestParams.getOwnerProposal());
    }

    if (actionRequestParams.hasDomainProposal()) {
      return buildDomainProposalParameters(actionRequestParams.getDomainProposal());
    }

    return Collections.emptyMap();
  }

  @Nonnull
  private Map<String, Object> buildGlossaryTermProposalParameters(
      @Nonnull final GlossaryTermProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(GLOSSARY_TERM_URN_KEY, proposal.getGlossaryTerm().toString());
    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildTagProposalParameters(@Nonnull final TagProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(TAG_URN_KEY, proposal.getTag().toString());
    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildCreateGlossaryNodeProposalParameters(
      @Nonnull final CreateGlossaryNodeProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(GLOSSARY_ENTITY_NAME_KEY, proposal.getName());
    if (proposal.hasParentNode()) {
      parameters.put(PARENT_NODE_URN_KEY, proposal.getParentNode().toString());
    }
    if (proposal.hasDescription()) {
      parameters.put(DESCRIPTION_KEY, proposal.getDescription());
    }
    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildCreateGlossaryTermProposalParameters(
      @Nonnull final CreateGlossaryTermProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(GLOSSARY_ENTITY_NAME_KEY, proposal.getName());
    if (proposal.hasParentNode()) {
      parameters.put(PARENT_NODE_URN_KEY, proposal.getParentNode().toString());
    }
    if (proposal.hasDescription()) {
      parameters.put(DESCRIPTION_KEY, proposal.getDescription());
    }
    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildUpdateDescriptionProposalParameters(
      @Nonnull final DescriptionProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();
    parameters.put(DESCRIPTION_KEY, proposal.getDescription());
    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildStructuredPropertyProposalParameters(
      @Nonnull final StructuredPropertyProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();

    if (proposal.hasStructuredPropertyValues()
        && !proposal.getStructuredPropertyValues().isEmpty()) {
      ObjectMapper objectMapper = new ObjectMapper();
      ArrayNode propertiesArray = objectMapper.createArrayNode();

      StructuredPropertyValueAssignment assignment = proposal.getStructuredPropertyValues().get(0);
      ObjectNode propertyNode = objectMapper.createObjectNode();
      propertyNode.put("propertyUrn", assignment.getPropertyUrn().toString());

      if (assignment.hasValues()) {
        ArrayNode valuesArray = objectMapper.createArrayNode();
        assignment
            .getValues()
            .forEach(
                value -> {
                  if (value.isString()) {
                    valuesArray.add(value.getString());
                  } else if (value.isDouble()) {
                    valuesArray.add(value.getDouble());
                  } else if (value.isNull()) {
                    valuesArray.addNull();
                  } else {
                    throw new RuntimeException(
                        "Unsupported structured property value type: " + value);
                  }
                });
        propertyNode.set("values", valuesArray);
      }

      propertiesArray.add(propertyNode);
      parameters.put("structuredProperties", propertiesArray.toString());
    }

    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildDomainProposalParameters(
      @Nonnull final DomainProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();
    ObjectMapper objectMapper = new ObjectMapper();
    ArrayNode arrayNode = objectMapper.createArrayNode();

    proposal.getDomains().forEach(domain -> arrayNode.add(domain.toString()));

    parameters.put("domains", arrayNode.toString());
    return parameters;
  }

  @Nonnull
  private Map<String, Object> buildOwnerProposalParameters(@Nonnull final OwnerProposal proposal) {
    Map<String, Object> parameters = new HashMap<>();

    if (proposal.hasOwners()) {
      ObjectMapper objectMapper = new ObjectMapper();
      ArrayNode ownersArray = objectMapper.createArrayNode();

      proposal
          .getOwners()
          .forEach(
              owner -> {
                ObjectNode ownerNode = objectMapper.createObjectNode();
                ownerNode.put("type", owner.getType().toString()); // Changed from ownershipType
                ownerNode.put(
                    "typeUrn", owner.getTypeUrn().toString()); // Changed from ownershipTypeUrn
                ownerNode.put("ownerUrn", owner.getOwner().toString());
                ownersArray.add(ownerNode);
              });

      parameters.put("owners", ownersArray.toString());
    }
    return parameters;
  }
}
