package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.UnlinkPhysicalChildInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.logical.LogicalModelUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UnlinkPhysicalChildResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final UnlinkPhysicalChildInput input =
        bindArgument(environment.getArgument("input"), UnlinkPhysicalChildInput.class);
    final Urn childUrn = UrnUtils.getUrn(input.getChildUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final List<String> childFieldPaths =
                SchemaMetadataUtils.fieldPathsOf(
                    _entityClient, context.getOperationContext(), childUrn);
            final List<MetadataChangeProposal> proposals =
                LogicalModelUtils.buildUnlinkProposals(
                    childUrn, childFieldPaths, context.getOperationContext());
            _entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);
            return true;
          } catch (Exception e) {
            log.error("Failed to unlink {}", childUrn, e);
            throw new RuntimeException(
                String.format("Failed to unlink physical child %s", childUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
