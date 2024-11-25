package com.linkedin.datahub.graphql.resolvers.datacontract;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datacontract.DataContractProperties;
import com.linkedin.datacontract.DataContractState;
import com.linkedin.datacontract.DataContractStatus;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.DataQualityContractArray;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.FreshnessContractArray;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datacontract.SchemaContractArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.DataContract;
import com.linkedin.datahub.graphql.generated.DataQualityContractInput;
import com.linkedin.datahub.graphql.generated.FreshnessContractInput;
import com.linkedin.datahub.graphql.generated.SchemaContractInput;
import com.linkedin.datahub.graphql.generated.UpsertDataContractInput;
import com.linkedin.datahub.graphql.types.datacontract.DataContractMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.DataContractKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UpsertDataContractResolver implements DataFetcher<CompletableFuture<DataContract>> {

  private static final DataContractState DEFAULT_CONTRACT_STATE = DataContractState.ACTIVE;
  private static final String CONTRACT_RELATIONSHIP_TYPE = "ContractFor";
  private final EntityClient _entityClient;
  private final GraphClient _graphClient;

  public UpsertDataContractResolver(
      final EntityClient entityClient, final GraphClient graphClient) {
    _entityClient = Objects.requireNonNull(entityClient, "entityClient cannot be null");
    _graphClient = Objects.requireNonNull(graphClient, "graphClient cannot be null");
  }

  @Override
  public CompletableFuture<DataContract> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final UpsertDataContractInput input =
        bindArgument(environment.getArgument("input"), UpsertDataContractInput.class);
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    return CompletableFuture.supplyAsync(
        () -> {
          if (DataContractUtils.canEditDataContract(context, entityUrn)) {

            // Verify that the provided contract, dataset, assertions all exist as valid entities.
            validateInput(entityUrn, input, context);

            // First determine if there is an existing data contract
            final Urn maybeExistingContractUrn =
                getEntityContractUrn(entityUrn, context.getAuthentication());

            final DataContractProperties newProperties = mapInputToProperties(entityUrn, input);
            final DataContractStatus newStatus = mapInputToStatus(input);

            final Urn urn =
                maybeExistingContractUrn != null
                    ? maybeExistingContractUrn
                    : EntityKeyUtils.convertEntityKeyToUrn(
                        new DataContractKey()
                            .setId(
                                input.getId() != null
                                    ? input.getId()
                                    : UUID.randomUUID().toString()),
                        Constants.DATA_CONTRACT_ENTITY_NAME);

            final MetadataChangeProposal propertiesProposal =
                buildMetadataChangeProposalWithUrn(
                    urn, Constants.DATA_CONTRACT_PROPERTIES_ASPECT_NAME, newProperties);

            final MetadataChangeProposal statusProposal =
                buildMetadataChangeProposalWithUrn(
                    urn, Constants.DATA_CONTRACT_STATUS_ASPECT_NAME, newStatus);

            try {
              _entityClient.batchIngestProposals(
                  context.getOperationContext(),
                  ImmutableList.of(propertiesProposal, statusProposal),
                  false);

              //  Hydrate the contract entities based on the urns from step 1
              final EntityResponse entityResponse =
                  _entityClient.getV2(
                      context.getOperationContext(),
                      Constants.DATA_CONTRACT_ENTITY_NAME,
                      urn,
                      null);

              // Package and return result
              return DataContractMapper.mapContract(entityResponse);
            } catch (Exception e) {
              throw new RuntimeException(
                  String.format("Failed to perform update against input %s", input.toString()), e);
            }
          }
          throw new AuthorizationException(
              "Unauthorized to perform this action. Please contact your DataHub administrator.");
        });
  }

  private void validateInput(
      @Nonnull final Urn entityUrn,
      @Nonnull final UpsertDataContractInput input,
      @Nonnull final QueryContext context) {
    try {

      // Validate the target entity exists
      if (!_entityClient.exists(context.getOperationContext(), entityUrn)) {
        throw new DataHubGraphQLException(
            String.format("Provided entity with urn %s does not exist!", entityUrn),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }

      // Verify Freshness assertions
      if (input.getFreshness() != null) {
        final List<FreshnessContractInput> freshnessInputs = input.getFreshness();
        for (FreshnessContractInput freshnessInput : freshnessInputs) {
          final Urn assertionUrn = UrnUtils.getUrn(freshnessInput.getAssertionUrn());
          if (!_entityClient.exists(context.getOperationContext(), assertionUrn)) {
            throw new DataHubGraphQLException(
                String.format("Provided assertion with urn %s does not exist!", assertionUrn),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }
        }
      }

      // Verify Schema assertions
      if (input.getSchema() != null) {
        final List<SchemaContractInput> schemaInputs = input.getSchema();
        for (SchemaContractInput schemaInput : schemaInputs) {
          final Urn assertionUrn = UrnUtils.getUrn(schemaInput.getAssertionUrn());
          if (!_entityClient.exists(context.getOperationContext(), assertionUrn)) {
            throw new DataHubGraphQLException(
                String.format("Provided assertion with urn %s does not exist!", assertionUrn),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }
        }
      }

      // Verify DQ assertions
      if (input.getDataQuality() != null) {
        final List<DataQualityContractInput> dqInputs = input.getDataQuality();
        for (DataQualityContractInput dqInput : dqInputs) {
          final Urn assertionUrn = UrnUtils.getUrn(dqInput.getAssertionUrn());
          if (!_entityClient.exists(context.getOperationContext(), assertionUrn)) {
            throw new DataHubGraphQLException(
                String.format("Provided assertion with urn %s does not exist!", assertionUrn),
                DataHubGraphQLErrorCode.BAD_REQUEST);
          }
        }
      }
    } catch (Exception e) {
      if (e instanceof DataHubGraphQLException) {
        throw (DataHubGraphQLException) e;
      } else {
        log.error(
            "Failed to validate inputs provided when upserting data contract! Failing the create.",
            e);
        throw new DataHubGraphQLException(
            "Failed to verify inputs. An unknown error occurred!",
            DataHubGraphQLErrorCode.SERVER_ERROR);
      }
    }
  }

  @Nullable
  private Urn getEntityContractUrn(@Nonnull Urn entityUrn, @Nonnull Authentication authentication) {
    EntityRelationships relationships =
        _graphClient.getRelatedEntities(
            entityUrn.toString(),
            ImmutableList.of(CONTRACT_RELATIONSHIP_TYPE),
            RelationshipDirection.INCOMING,
            0,
            1,
            authentication.getActor().toUrnStr());

    if (relationships.getTotal() > 1) {
      // Bad state - There are multiple contracts for a single entity! Cannot update.
      log.warn(
          String.format(
              "Unexpectedly found multiple contracts (%s) for entity with urn %s! This may lead to inconsistent behavior.",
              relationships.getRelationships(), entityUrn));
    }

    if (relationships.getRelationships().size() == 1) {
      return relationships.getRelationships().get(0).getEntity();
    }
    // No Contract Found
    return null;
  }

  private DataContractProperties mapInputToProperties(
      @Nonnull final Urn entityUrn, @Nonnull final UpsertDataContractInput input) {
    final DataContractProperties result = new DataContractProperties();
    result.setEntity(entityUrn);

    // Construct the dataset contract.
    if (input.getFreshness() != null) {
      result.setFreshness(
          new FreshnessContractArray(
              input.getFreshness().stream()
                  .map(this::mapFreshnessInput)
                  .collect(Collectors.toList())));
    }

    if (input.getSchema() != null) {
      result.setSchema(
          new SchemaContractArray(
              input.getSchema().stream().map(this::mapSchemaInput).collect(Collectors.toList())));
    }

    if (input.getDataQuality() != null) {
      result.setDataQuality(
          new DataQualityContractArray(
              input.getDataQuality().stream()
                  .map(this::mapDataQualityInput)
                  .collect(Collectors.toList())));
    }

    return result;
  }

  private DataContractStatus mapInputToStatus(@Nonnull final UpsertDataContractInput input) {
    final DataContractStatus result = new DataContractStatus();
    if (input.getState() != null) {
      result.setState(DataContractState.valueOf(input.getState().toString()));
    } else {
      result.setState(DEFAULT_CONTRACT_STATE);
    }
    return result;
  }

  private FreshnessContract mapFreshnessInput(@Nonnull final FreshnessContractInput input) {
    final FreshnessContract result = new FreshnessContract();
    result.setAssertion(UrnUtils.getUrn(input.getAssertionUrn()));
    return result;
  }

  private SchemaContract mapSchemaInput(@Nonnull final SchemaContractInput input) {
    final SchemaContract result = new SchemaContract();
    result.setAssertion(UrnUtils.getUrn(input.getAssertionUrn()));
    return result;
  }

  private DataQualityContract mapDataQualityInput(@Nonnull final DataQualityContractInput input) {
    final DataQualityContract result = new DataQualityContract();
    result.setAssertion(UrnUtils.getUrn(input.getAssertionUrn()));
    return result;
  }
}
