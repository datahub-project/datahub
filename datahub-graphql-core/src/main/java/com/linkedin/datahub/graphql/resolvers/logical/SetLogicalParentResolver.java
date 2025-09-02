package com.linkedin.datahub.graphql.resolvers.logical;

import static com.linkedin.data.template.SetMode.*;
import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.Edge;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.generated.SetLogicalParentInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.logical.LogicalParent;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class SetLogicalParentResolver implements DataFetcher<CompletableFuture<Boolean>> {
  private final EntityClient _entityClient;

  @Override
  public CompletableFuture<Boolean> get(DataFetchingEnvironment environment) throws Exception {
    // Note: No validation on existence of parent urn

    final QueryContext context = environment.getContext();
    final SetLogicalParentInput input =
        bindArgument(environment.getArgument("input"), SetLogicalParentInput.class);
    final Urn entityUrn = Urn.createFromString(input.getResourceUrn());
    @Nullable final String parent = input.getParentUrn();
    LogicalParent logicalParent = createLogicalParent(parent, context);
    final MetadataChangeProposal proposal =
        buildMetadataChangeProposalWithUrn(entityUrn, LOGICAL_PARENT_ASPECT_NAME, logicalParent);

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          try {
            _entityClient.ingestProposal(context.getOperationContext(), proposal, false);
            return true;
          } catch (Exception e) {
            log.error(
                "Failed to set Logical Parent on entity urn {} to {}: {}",
                entityUrn,
                parent,
                e.getMessage());
            throw new RuntimeException(
                String.format("Failed to set Logical Parent on entity %s to %s", entityUrn, parent),
                e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }

  private LogicalParent createLogicalParent(@Nullable String parent, QueryContext context)
      throws URISyntaxException {
    if (parent == null) {
      return new LogicalParent().setParent(null, REMOVE_IF_NULL);
    }

    Urn parentUrn = Urn.createFromString(parent);
    Urn actor = Urn.createFromString(context.getActorUrn());
    long now = System.currentTimeMillis();
    Edge edge =
        new Edge()
            .setDestinationUrn(parentUrn)
            .setCreated(new AuditStamp().setTime(now).setActor(actor))
            .setLastModified(new AuditStamp().setTime(now).setActor(actor));

    return new LogicalParent().setParent(edge);
  }
}
