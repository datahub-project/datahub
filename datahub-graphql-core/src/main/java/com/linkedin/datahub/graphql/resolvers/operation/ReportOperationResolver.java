package com.linkedin.datahub.graphql.resolvers.operation;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.Operation;
import com.linkedin.common.OperationSourceType;
import com.linkedin.common.OperationType;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.SetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.authorization.ConjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.authorization.DisjunctivePrivilegeGroup;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ReportOperationInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.timeseries.PartitionSpec;
import com.linkedin.timeseries.PartitionType;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.datahub.graphql.resolvers.AuthUtils.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

/**
 * Resolver used for reporting Asset Operations
 */
@Slf4j
@RequiredArgsConstructor
public class ReportOperationResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private static final List<String> SUPPORTED_ENTITY_TYPES = ImmutableList.of(
      Constants.DATASET_ENTITY_NAME
  );

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();
    final ReportOperationInput input = bindArgument(environment.getArgument("input"), ReportOperationInput.class);

    return CompletableFuture.supplyAsync(() -> {

      Urn entityUrn = UrnUtils.getUrn(input.getUrn());

      if (!isAuthorizedToReportOperationForResource(entityUrn, context)) {
        throw new AuthorizationException("Unauthorized to perform this action. Please contact your DataHub administrator.");
      }

      validateInput(entityUrn, input);

      try {
        // Create an MCP to emit the operation
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        proposal.setEntityUrn(entityUrn);
        proposal.setEntityType(entityUrn.getEntityType());
        proposal.setAspectName(Constants.OPERATION_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(mapOperation(input, context)));
        proposal.setChangeType(ChangeType.UPSERT);
        _entityClient.ingestProposal(proposal, context.getAuthentication());
        return true;
      } catch (Exception e) {
        log.error("Failed to report operation. {}", e.getMessage());
        throw new RuntimeException("Failed to report operation", e);
      }
    });
  }

  private Operation mapOperation(final ReportOperationInput input, final QueryContext context) throws URISyntaxException {

    final Operation result = new Operation();
    result.setActor(UrnUtils.getUrn(context.getActorUrn()));
    result.setOperationType(OperationType.valueOf(input.getOperationType().toString()));
    result.setCustomOperationType(input.getCustomOperationType(), SetMode.IGNORE_NULL);
    result.setNumAffectedRows(input.getNumAffectedRows(), SetMode.IGNORE_NULL);

    long timestampMillis = input.getTimestampMillis() != null ? input.getTimestampMillis() : System.currentTimeMillis();
    result.setLastUpdatedTimestamp(timestampMillis);
    result.setTimestampMillis(timestampMillis);
    result.setSourceType(OperationSourceType.valueOf(input.getSourceType().toString()));

    if (input.getPartition() != null) {
      result.setPartitionSpec(new PartitionSpec().setType(PartitionType.PARTITION).setPartition(input.getPartition()));
    }

    if (input.getCustomProperties() != null) {
      result.setCustomProperties(mapCustomProperties(input.getCustomProperties()));
    }

    return result;
  }

  private StringMap mapCustomProperties(final List<StringMapEntryInput> properties) throws URISyntaxException {
    final StringMap result = new StringMap();
    for (StringMapEntryInput entry : properties) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  private void validateInput(final Urn entityUrn, final ReportOperationInput input) {
    if (!SUPPORTED_ENTITY_TYPES.contains(entityUrn.getEntityType())) {
      throw new DataHubGraphQLException(
          String.format("Unable to report operation. Invalid entity type %s provided.", entityUrn.getEntityType()),
          DataHubGraphQLErrorCode.BAD_REQUEST);
    }
  }

  private boolean isAuthorizedToReportOperationForResource(final Urn resourceUrn, final QueryContext context) {
    final DisjunctivePrivilegeGroup orPrivilegeGroups = new DisjunctivePrivilegeGroup(ImmutableList.of(
        ALL_PRIVILEGES_GROUP,
        new ConjunctivePrivilegeGroup(ImmutableList.of(PoliciesConfig.EDIT_ENTITY_OPERATIONS_PRIVILEGE.getType()))
    ));

    return AuthorizationUtils.isAuthorized(
        context.getAuthorizer(),
        context.getActorUrn(),
        resourceUrn.getEntityType(),
        resourceUrn.toString(),
        orPrivilegeGroups);
  }
}