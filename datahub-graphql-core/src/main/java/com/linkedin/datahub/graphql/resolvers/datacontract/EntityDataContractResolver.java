package com.linkedin.datahub.graphql.resolvers.datacontract;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.types.datacontract.DataContractMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityDataContractResolver implements DataFetcher<CompletableFuture<DataContract>> {
  static final String CONTRACT_FOR_RELATIONSHIP = "ContractFor";

  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  public EntityDataContractResolver(
      final EntityClient entityClient, final GraphClient graphClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient must not be null");
    _graphClient = Objects.requireNonNull(graphClient, "graphClient must not be null");
  }

  @Override
  public CompletableFuture<DataContract> get(DataFetchingEnvironment environment) {
    return CompletableFuture.supplyAsync(
        () -> {
          final QueryContext context = environment.getContext();
          final String entityUrn = ((Entity) environment.getSource()).getUrn();

          try {
            // Step 1: Fetch the contract associated with the dataset.
            final EntityRelationships relationships =
                _graphClient.getRelatedEntities(
                    entityUrn,
                    ImmutableList.of(CONTRACT_FOR_RELATIONSHIP),
                    RelationshipDirection.INCOMING,
                    0,
                    1,
                    context.getActorUrn());

            // If we found multiple contracts for same entity, we have an invalid system state. Log
            // a warning.
            if (relationships.getTotal() > 1) {
              // Someone created 2 contracts for the same entity. Currently, we do not handle this
              // in the UI.
              log.warn(
                  String.format(
                      "Unexpectedly found multiple contracts (%s) for entity with urn %s! This may lead to inconsistent behavior.",
                      relationships.getRelationships(), entityUrn));
            }

            final List<Urn> contractUrns =
                relationships.getRelationships().stream()
                    .map(EntityRelationship::getEntity)
                    .collect(Collectors.toList());

            if (!contractUrns.isEmpty()) {
              final Urn contractUrn = contractUrns.get(0);

              // Step 2: Hydrate the contract entities based on the urns from step 1
              final EntityResponse entityResponse =
                  _entityClient.getV2(
                      context.getOperationContext(),
                      Constants.DATA_CONTRACT_ENTITY_NAME,
                      contractUrn,
                      null);

              if (entityResponse != null) {
                // Step 4: Package and return result
                return DataContractMapper.mapContract(entityResponse);
              }
            }
            // No contract found
            return null;
          } catch (URISyntaxException | RemoteInvocationException e) {
            throw new RuntimeException("Failed to retrieve Data Contract from GMS", e);
          }
        });
  }
}
