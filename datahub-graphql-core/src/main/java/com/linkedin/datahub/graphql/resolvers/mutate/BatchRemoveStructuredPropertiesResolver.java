package com.linkedin.datahub.graphql.resolvers.mutate;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.STRUCTURED_PROPERTIES_ASPECT_NAME;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.BatchRemoveStructuredPropertiesInput;
import com.linkedin.datahub.graphql.generated.ResourceRefInput;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.AspectUtils;
import com.linkedin.metadata.utils.AuditStampUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.structured.StructuredProperties;
import com.linkedin.structured.StructuredPropertyValueAssignment;
import com.linkedin.structured.StructuredPropertyValueAssignmentArray;
import graphql.com.google.common.collect.ImmutableSet;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchRemoveStructuredPropertiesResolver
    implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final BatchRemoveStructuredPropertiesInput input =
        bindArgument(environment.getArgument("input"), BatchRemoveStructuredPropertiesInput.class);
    final List<String> structuredPropertyUrns = input.getStructuredPropertyUrns();
    final List<ResourceRefInput> resources = input.getResources();
    final QueryContext context = environment.getContext();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          validateInputResources(context.getOperationContext(), resources, context);

          try {
            batchRemoveStructuredProperties(
                context.getOperationContext(), structuredPropertyUrns, resources, context);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to perform update against input {}, {}", input.toString(), e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private void validateInputResources(
      @Nonnull OperationContext opContext, List<ResourceRefInput> resources, QueryContext context) {
    for (ResourceRefInput resource : resources) {
      validateInputResource(opContext, resource, context);
    }
  }

  private void validateInputResource(
      @Nonnull OperationContext opContext, ResourceRefInput resource, QueryContext context) {
    final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());

    if (resource.getSubResource() != null) {
      throw new IllegalArgumentException(
          "Malformed input provided: structured properties cannot be applied to subresources.");
    }

    if (!AuthorizationUtils.canEditProperties(resourceUrn, context)) {
      throw new AuthorizationException(
          String.format("Not authorized to edit properties on entity %s", resourceUrn));
    }

    try {
      if (!_entityClient.exists(opContext, resourceUrn)) {
        throw new IllegalArgumentException(
            String.format("Entity with urn %s does not exist.", resourceUrn));
      }
    } catch (RemoteInvocationException e) {
      throw new RuntimeException(
          String.format("Failed to validate entity existence for urn %s", resourceUrn), e);
    }
  }

  private void batchRemoveStructuredProperties(
      @Nonnull OperationContext opContext,
      List<String> structuredPropertyUrns,
      List<ResourceRefInput> resources,
      QueryContext context) {
    log.debug(
        "Batch removing structured properties. properties: {}, resources: {}",
        structuredPropertyUrns,
        resources);

    final AuditStamp auditStamp =
        AuditStampUtils.createAuditStamp(context.getAuthentication().getActor().toUrnStr());
    final Set<String> propertyUrnsToRemove =
        structuredPropertyUrns.stream().collect(Collectors.toSet());

    try {
      for (ResourceRefInput resource : resources) {
        final Urn resourceUrn = UrnUtils.getUrn(resource.getResourceUrn());

        StructuredProperties existingStructuredProperties =
            getStructuredProperties(opContext, resourceUrn);

        Map<String, StructuredPropertyValueAssignment> propertyMap = new HashMap<>();
        if (existingStructuredProperties.hasProperties()) {
          for (StructuredPropertyValueAssignment assignment :
              existingStructuredProperties.getProperties()) {
            propertyMap.put(assignment.getPropertyUrn().toString(), assignment);
          }
        }

        boolean hasChanges = false;
        for (String propertyUrnStr : propertyUrnsToRemove) {
          if (propertyMap.containsKey(propertyUrnStr)) {
            propertyMap.remove(propertyUrnStr);
            hasChanges = true;
          }
        }

        if (hasChanges) {
          StructuredPropertyValueAssignmentArray updatedProperties =
              new StructuredPropertyValueAssignmentArray(propertyMap.values());
          existingStructuredProperties.setProperties(updatedProperties);

          final MetadataChangeProposal proposal =
              AspectUtils.buildMetadataChangeProposal(
                  resourceUrn, STRUCTURED_PROPERTIES_ASPECT_NAME, existingStructuredProperties);

          _entityClient.ingestProposal(opContext, proposal, false);
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to batch remove structured properties %s from resources with urns %s!",
              structuredPropertyUrns,
              resources.stream()
                  .map(ResourceRefInput::getResourceUrn)
                  .collect(Collectors.toList())),
          e);
    }
  }

  private StructuredProperties getStructuredProperties(
      @Nonnull OperationContext opContext, Urn entityUrn) throws Exception {
    EntityResponse response =
        _entityClient.getV2(
            opContext,
            entityUrn.getEntityType(),
            entityUrn,
            ImmutableSet.of(STRUCTURED_PROPERTIES_ASPECT_NAME));

    StructuredProperties structuredProperties = new StructuredProperties();
    structuredProperties.setProperties(new StructuredPropertyValueAssignmentArray());

    if (response != null && response.getAspects().containsKey(STRUCTURED_PROPERTIES_ASPECT_NAME)) {
      structuredProperties =
          new StructuredProperties(
              response.getAspects().get(STRUCTURED_PROPERTIES_ASPECT_NAME).getValue().data());
    }

    return structuredProperties;
  }
}
