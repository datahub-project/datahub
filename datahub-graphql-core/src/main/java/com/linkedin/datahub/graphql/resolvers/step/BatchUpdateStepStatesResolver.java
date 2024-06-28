package com.linkedin.datahub.graphql.resolvers.step;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.metadata.Constants.*;
import static com.linkedin.metadata.entity.AspectUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.BatchUpdateStepStatesInput;
import com.linkedin.datahub.graphql.generated.BatchUpdateStepStatesResult;
import com.linkedin.datahub.graphql.generated.StepStateInput;
import com.linkedin.datahub.graphql.generated.StringMapEntryInput;
import com.linkedin.datahub.graphql.generated.UpdateStepStateResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.DataHubStepStateKey;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.step.DataHubStepStateProperties;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class BatchUpdateStepStatesResolver
    implements DataFetcher<CompletableFuture<BatchUpdateStepStatesResult>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<BatchUpdateStepStatesResult> get(
      @Nonnull final DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final Authentication authentication = context.getAuthentication();

    final BatchUpdateStepStatesInput input =
        bindArgument(environment.getArgument("input"), BatchUpdateStepStatesInput.class);
    final List<StepStateInput> states = input.getStates();
    final String actorUrnStr = authentication.getActor().toUrnStr();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          final Urn actorUrn = UrnUtils.getUrn(actorUrnStr);
          final AuditStamp auditStamp =
              new AuditStamp().setActor(actorUrn).setTime(System.currentTimeMillis());
          final List<UpdateStepStateResult> results =
              states.stream()
                  .map(
                      state ->
                          buildUpdateStepStateResult(
                              context.getOperationContext(), state, auditStamp))
                  .collect(Collectors.toList());
          final BatchUpdateStepStatesResult result = new BatchUpdateStepStatesResult();
          result.setResults(results);
          return result;
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private UpdateStepStateResult buildUpdateStepStateResult(
      @Nonnull OperationContext opContext,
      @Nonnull final StepStateInput state,
      @Nonnull final AuditStamp auditStamp) {
    final String id = state.getId();
    final UpdateStepStateResult updateStepStateResult = new UpdateStepStateResult();
    updateStepStateResult.setId(id);
    final boolean success = updateStepState(opContext, id, state.getProperties(), auditStamp);
    updateStepStateResult.setSucceeded(success);
    return updateStepStateResult;
  }

  private boolean updateStepState(
      @Nonnull OperationContext opContext,
      @Nonnull final String id,
      @Nonnull final List<StringMapEntryInput> inputProperties,
      @Nonnull final AuditStamp auditStamp) {
    final Map<String, String> properties =
        inputProperties.stream()
            .collect(Collectors.toMap(StringMapEntryInput::getKey, StringMapEntryInput::getValue));
    try {
      final DataHubStepStateKey stepStateKey = new DataHubStepStateKey().setId(id);
      final DataHubStepStateProperties stepStateProperties =
          new DataHubStepStateProperties()
              .setProperties(new StringMap(properties))
              .setLastModified(auditStamp);

      final MetadataChangeProposal proposal =
          buildMetadataChangeProposal(
              DATAHUB_STEP_STATE_ENTITY_NAME,
              stepStateKey,
              DATAHUB_STEP_STATE_PROPERTIES_ASPECT_NAME,
              stepStateProperties);
      _entityClient.ingestProposal(opContext, proposal, false);
      return true;
    } catch (Exception e) {
      log.error("Could not update step state for id {}", id, e);
      return false;
    }
  }
}
