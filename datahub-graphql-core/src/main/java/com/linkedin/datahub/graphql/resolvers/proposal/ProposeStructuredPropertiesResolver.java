package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrn;
import static com.linkedin.datahub.graphql.resolvers.proposal.ProposalUtils.toUrns;

import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.PropertyValueInput;
import com.linkedin.datahub.graphql.generated.ProposeStructuredPropertiesInput;
import com.linkedin.datahub.graphql.generated.StructuredPropertyInputParams;
import com.linkedin.datahub.graphql.generated.SubResourceType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.service.ActionRequestService;
import com.linkedin.metadata.service.EntityDoesNotExistException;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.PrimitivePropertyValue;
import com.linkedin.structured.PrimitivePropertyValueArray;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ProposeStructuredPropertiesResolver implements DataFetcher<CompletableFuture<String>> {

  private final ActionRequestService actionRequestService;

  @Override
  public CompletableFuture<String> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    // Bind Inputs
    final ProposeStructuredPropertiesInput input =
        bindArgument(environment.getArgument("input"), ProposeStructuredPropertiesInput.class);
    final String targetUrnStr = input.getResourceUrn();
    final List<StructuredPropertyInputParams> propertyParams = input.getStructuredProperties();
    final String description = input.getDescription();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          // Step 1: validate and authorize the action
          validateAndAuthorize(context, propertyParams, targetUrnStr, input);

          // Step 2: raise the proposal.
          try {
            if (SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())) {
              // Propose Schema Field Structured Properties
              return this.actionRequestService
                  .proposeSchemaFieldStructuredProperties(
                      context.getOperationContext(),
                      toUrn(targetUrnStr),
                      input.getSubResource(),
                      toStructuredPropertyValueAssignments(propertyParams),
                      description)
                  .toString();
            } else {
              // Propose Asset Structured Properties
              return this.actionRequestService
                  .proposeEntityStructuredProperties(
                      context.getOperationContext(),
                      toUrn(targetUrnStr),
                      toStructuredPropertyValueAssignments(propertyParams),
                      description)
                  .toString();
            }
          } catch (Exception e) {
            if (e instanceof ActionRequestService.MalformedActionRequestException) {
              throw new DataHubGraphQLException(
                  "Failed to propose structured properties: " + e.getMessage(),
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyAppliedException) {
              throw new DataHubGraphQLException(
                  "Structured properties are already applied to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof ActionRequestService.AlreadyRequestedException) {
              throw new DataHubGraphQLException(
                  "Structured properties have already been proposed to target resource!",
                  DataHubGraphQLErrorCode.BAD_REQUEST);
            } else if (e instanceof RemoteInvocationException) {
              throw new DataHubGraphQLException(
                  "Failed to create structured properties proposal. Encountered an error while attempting to reach the downstream service: "
                      + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR);
            } else if (e instanceof EntityDoesNotExistException) {
              throw new DataHubGraphQLException(
                  "Failed to create structured property proposal: " + e.getMessage(),
                  DataHubGraphQLErrorCode.NOT_FOUND);
            } else {
              throw new DataHubGraphQLException(
                  "Failed to propose structured properties: " + e.getMessage(),
                  DataHubGraphQLErrorCode.SERVER_ERROR,
                  e);
            }
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateAndAuthorize(
      QueryContext context,
      List<StructuredPropertyInputParams> propertiesParams,
      String targetUrnStr,
      ProposeStructuredPropertiesInput input) {
    try {
      toUrn(targetUrnStr);
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid resource urn provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    try {
      toUrns(
          propertiesParams.stream()
              .map(StructuredPropertyInputParams::getStructuredPropertyUrn)
              .collect(Collectors.toList()));
    } catch (RuntimeException e) {
      throw new DataHubGraphQLException(
          "Invalid structured property urns provided.", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!ProposalUtils.isAuthorizedToProposeStructuredProperties(
        context, toUrn(targetUrnStr), input.getSubResource())) {
      throw new AuthorizationException(
          "Unauthorized to perform this action. Please contact your DataHub administrator.");
    }
    if (propertiesParams.isEmpty()) {
      throw new DataHubGraphQLException(
          "No structured properties provided to propose", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!propertiesParams.stream()
        .allMatch(
            propertyParams ->
                UrnUtils.getUrn(propertyParams.getStructuredPropertyUrn())
                    .getEntityType()
                    .equals(Constants.STRUCTURED_PROPERTY_ENTITY_NAME))) {
      throw new DataHubGraphQLException(
          "All provided properties must be of type 'structuredProperty'",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (!propertiesParams.stream()
        .filter(propertyParams -> !propertyParams.getValues().isEmpty())
        .flatMap(propertyParams -> propertyParams.getValues().stream())
        .allMatch(
            propertyValue ->
                propertyValue.getNumberValue() != null || propertyValue.getStringValue() != null)) {
      throw new DataHubGraphQLException(
          "Some properties are missing string or numeric values. One must be provided!",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())
        && input.getSubResource() == null) {
      throw new DataHubGraphQLException(
          "subResource field must be provided for proposing structured properties on dataset fields",
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
    if (input.getSubResource() != null
        && !SubResourceType.DATASET_FIELD.equals(input.getSubResourceType())) {
      throw new DataHubGraphQLException(
          "Unsupported subResourceType type 'none' provided", DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }

  private List<StructuredPropertyValueAssignment> toStructuredPropertyValueAssignments(
      @Nonnull final List<StructuredPropertyInputParams> propertyParams) {
    return propertyParams.stream()
        .map(
            propertyParam -> {
              final StructuredPropertyValueAssignment structuredProperty =
                  new StructuredPropertyValueAssignment();
              structuredProperty.setPropertyUrn(
                  UrnUtils.getUrn(propertyParam.getStructuredPropertyUrn()));
              structuredProperty.setValues(
                  new PrimitivePropertyValueArray(
                      toStructuredPropertyValues(propertyParam.getValues())));
              return structuredProperty;
            })
        .collect(Collectors.toList());
  }

  private List<PrimitivePropertyValue> toStructuredPropertyValues(
      @Nonnull final List<PropertyValueInput> propertyValueInputs) {
    return propertyValueInputs.stream()
        .map(
            propertyValueInput -> {
              final PrimitivePropertyValue primitivePropertyValue = new PrimitivePropertyValue();
              if (propertyValueInput.getNumberValue() != null) {
                // Case 1: Numeric property type.
                primitivePropertyValue.setDouble(propertyValueInput.getNumberValue().doubleValue());
              } else if (propertyValueInput.getStringValue() != null) {
                // Case 2: String property type.
                primitivePropertyValue.setString(propertyValueInput.getStringValue());
              } else {
                // Should never happen, since validation already happened before mapping.
                throw new DataHubGraphQLException(
                    "Property value must be either a string or a number!",
                    DataHubGraphQLErrorCode.BAD_REQUEST);
              }
              return primitivePropertyValue;
            })
        .collect(Collectors.toList());
  }
}
