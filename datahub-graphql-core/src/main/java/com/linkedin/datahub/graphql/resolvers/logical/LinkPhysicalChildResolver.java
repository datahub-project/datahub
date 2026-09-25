package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.ColumnMappingInput;
import com.linkedin.datahub.graphql.generated.LinkPhysicalChildInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.entity.logical.LogicalModelUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class LinkPhysicalChildResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();
    final LinkPhysicalChildInput input =
        bindArgument(environment.getArgument("input"), LinkPhysicalChildInput.class);
    final Urn parentUrn = UrnUtils.getUrn(input.getLogicalParentUrn());
    final Urn childUrn = UrnUtils.getUrn(input.getChildUrn());

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            final Map<String, String> fieldPathMap = new HashMap<>();
            if (input.getColumnMappings() != null) {
              for (ColumnMappingInput m : input.getColumnMappings()) {
                fieldPathMap.put(m.getParentFieldPath(), m.getChildFieldPath());
              }
            }
            final List<MetadataChangeProposal> proposals =
                LogicalModelUtils.buildLinkProposals(
                    childUrn, parentUrn, fieldPathMap, context.getOperationContext());
            _entityClient.batchIngestProposals(context.getOperationContext(), proposals, false);
            return true;
          } catch (Exception e) {
            log.error("Failed to link {} -> {}", childUrn, parentUrn, e);
            throw new RuntimeException(
                String.format("Failed to link physical child %s", childUrn), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
