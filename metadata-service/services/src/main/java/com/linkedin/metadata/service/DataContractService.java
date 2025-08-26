package com.linkedin.metadata.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.openapi.client.OpenApiClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataContractService extends BaseService {

  private static final String CONTRACT_FOR_RELATIONSHIP = "ContractFor";
  private final GraphClient _graphClient;

  public DataContractService(
      @Nonnull final SystemEntityClient entityClient,
      @Nonnull final GraphClient graphClient,
      @Nonnull final OpenApiClient openApiClient,
      @Nonnull ObjectMapper objectMapper) {
    super(entityClient, openApiClient, objectMapper);
    _graphClient = Objects.requireNonNull(graphClient, "graphClient must not be null");
  }

  /**
   * Retrieves a single Data Contract Associated with an Entity, returns null if no contract is
   * found.
   */
  @Nullable
  public Urn getEntityContractUrn(
      @Nonnull OperationContext opContext, @Nonnull final Urn entityUrn) {
    Objects.requireNonNull(entityUrn, "entityUrn must not be null");
    try {
      // Fetch the contract associated with the entity.
      final EntityRelationships relationships =
          _graphClient.getRelatedEntities(
              entityUrn.toString(),
              ImmutableSet.of(CONTRACT_FOR_RELATIONSHIP),
              RelationshipDirection.INCOMING,
              0,
              1,
              opContext.getActorContext().getActorUrn().toString());

      // If we found multiple contracts for same entity, we have an invalid system state. Log
      // a warning.
      if (relationships.getTotal() > 1) {
        log.warn(
            String.format(
                "Unexpectedly found multiple contracts (%s) for entity with urn %s! This may lead to inconsistent behavior.",
                relationships.getRelationships(), entityUrn));
      }

      final List<Urn> contractUrns =
          relationships.getRelationships().stream()
              .map(EntityRelationship::getEntity)
              .filter(contractUrn -> contractExists(contractUrn, false, opContext))
              .collect(Collectors.toList());

      if (contractUrns.size() >= 1) {
        return contractUrns.get(0);
      }
      // No contract found
      return null;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to retrieve Data Contract for entity with urn %s", entityUrn), e);
    }
  }

  /**
   * Retrieves a single Data Contract Associated with an Entity, returns null if no contract is
   * found.
   */
  @Nullable
  public DataContractProperties getDataContractProperties(
      @Nonnull OperationContext opContext, @Nonnull final Urn contractUrn) {
    try {
      final EntityResponse entityResponse =
          this.entityClient.getV2(
              opContext, Constants.DATA_CONTRACT_ENTITY_NAME, contractUrn, null);

      if (entityResponse != null
          && entityResponse
              .getAspects()
              .containsKey(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME)) {
        return new DataContractProperties(
            entityResponse
                .getAspects()
                .get(Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME)
                .getValue()
                .data());
      }
      return null;
    } catch (Exception e) {
      throw new RuntimeException(
          String.format(
              "Failed to retrieve Data Contract Properties for contract with urn %s", contractUrn),
          e);
    }
  }

  /** Retrieves the set of Assertion urns associated with a contract. */
  @Nullable
  public List<Urn> getDataContractAssertionUrns(
      @Nonnull OperationContext opContext, @Nonnull final Urn contractUrn) {
    DataContractProperties properties = getDataContractProperties(opContext, contractUrn);
    if (properties == null) {
      return null;
    }
    final List<Urn> assertionUrns = new ArrayList<>();
    if (properties.hasSchema()) {
      assertionUrns.addAll(
          properties.getSchema().stream()
              .map(SchemaContract::getAssertion)
              .collect(Collectors.toList()));
    }
    if (properties.hasDataQuality()) {
      assertionUrns.addAll(
          properties.getDataQuality().stream()
              .map(DataQualityContract::getAssertion)
              .collect(Collectors.toList()));
    }
    if (properties.hasFreshness()) {
      assertionUrns.addAll(
          properties.getFreshness().stream()
              .map(FreshnessContract::getAssertion)
              .collect(Collectors.toList()));
    }
    return assertionUrns;
  }

  private boolean contractExists(
      @Nonnull Urn urn, @Nonnull Boolean includeSoftDeleted, @Nonnull OperationContext opContext) {
    try {
      return this.entityClient.exists(opContext, urn, includeSoftDeleted);
    } catch (RemoteInvocationException e) {
      log.error(String.format("Unable to check if contract %s exists, ignoring it", urn), e);
      return false;
    }
  }
}
