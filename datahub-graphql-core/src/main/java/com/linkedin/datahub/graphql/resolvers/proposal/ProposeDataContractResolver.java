package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.actionrequest.DataContractProposalOperationType;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datacontract.DataQualityContract;
import com.linkedin.datacontract.FreshnessContract;
import com.linkedin.datacontract.SchemaContract;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataQualityContractInput;
import com.linkedin.datahub.graphql.generated.FreshnessContractInput;
import com.linkedin.datahub.graphql.generated.ProposeDataContractInput;
import com.linkedin.datahub.graphql.generated.SchemaContractInput;
import com.linkedin.metadata.service.ActionRequestService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class ProposeDataContractResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final ActionRequestService _proposalService;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final ProposeDataContractInput input =
        bindArgument(environment.getArgument("input"), ProposeDataContractInput.class);
    final QueryContext context = environment.getContext();
    final Urn entityUrn = UrnUtils.getUrn(input.getEntityUrn());
    final DataContractProposalOperationType opType =
        DataContractProposalOperationType.valueOf(input.getOperationType().toString());
    final List<FreshnessContract> freshness = mapFreshnessContracts(input.getFreshness());
    final List<SchemaContract> schema = mapSchemaContracts(input.getSchema());
    final List<DataQualityContract> quality = mapQualityContracts(input.getDataQuality());
    final Urn actorUrn = CorpuserUrn.createFromString(context.getActorUrn());

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            log.info("Proposing change to data contract. input: {}", input);
            return _proposalService.proposeDataContract(
                context.getOperationContext(),
                actorUrn,
                entityUrn,
                opType,
                freshness,
                schema,
                quality);
          } catch (Exception e) {
            log.error("Failed to perform update against input {}, {}", input, e.getMessage());
            throw new RuntimeException(
                String.format("Failed to perform update against input %s", input), e);
          }
        });
  }

  @Nullable
  private List<FreshnessContract> mapFreshnessContracts(
      @Nullable final List<FreshnessContractInput> input) {
    if (input == null) {
      return null;
    }
    return input.stream().map(this::mapFreshnessContract).collect(Collectors.toList());
  }

  @Nonnull
  private FreshnessContract mapFreshnessContract(@Nonnull FreshnessContractInput input) {
    final FreshnessContract result = new FreshnessContract();
    result.setAssertion(UrnUtils.getUrn(input.getAssertionUrn()));
    return result;
  }

  @Nullable
  private List<SchemaContract> mapSchemaContracts(@Nullable final List<SchemaContractInput> input) {
    if (input == null) {
      return null;
    }
    return input.stream().map(this::mapSchemaContract).collect(Collectors.toList());
  }

  @Nonnull
  private SchemaContract mapSchemaContract(@Nonnull final SchemaContractInput input) {
    final SchemaContract result = new SchemaContract();
    result.setAssertion(UrnUtils.getUrn(input.getAssertionUrn()));
    return result;
  }

  @Nullable
  private List<DataQualityContract> mapQualityContracts(
      @Nullable final List<DataQualityContractInput> input) {
    if (input == null) {
      return null;
    }
    return input.stream().map(this::mapQualityContract).collect(Collectors.toList());
  }

  @Nonnull
  private DataQualityContract mapQualityContract(@Nonnull final DataQualityContractInput input) {
    final DataQualityContract result = new DataQualityContract();
    result.setAssertion(UrnUtils.getUrn(input.getAssertionUrn()));
    return result;
  }
}
