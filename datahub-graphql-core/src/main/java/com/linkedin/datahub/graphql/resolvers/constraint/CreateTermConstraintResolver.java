package com.linkedin.datahub.graphql.resolvers.constraint;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;
import static com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils.*;
import static com.linkedin.metadata.Constants.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.constraint.ConstraintInfo;
import com.linkedin.constraint.ConstraintParams;
import com.linkedin.constraint.GlossaryTermInNodeConstraint;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.ConstraintType;
import com.linkedin.datahub.graphql.generated.CreateTermConstraintInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.key.ConstraintKey;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class CreateTermConstraintResolver implements DataFetcher<CompletableFuture<String>> {

  private final EntityClient _aspectClient;

  public CreateTermConstraintResolver(final EntityClient aspectClient) {
    _aspectClient = aspectClient;
  }

  @Override
  public CompletableFuture<String> get(final DataFetchingEnvironment environment) throws Exception {

    final QueryContext context = environment.getContext();

    if (ConstraintUtils.isAuthorizedToCreateConstraints(context)) {
      final CreateTermConstraintInput input =
          bindArgument(environment.getArgument("input"), CreateTermConstraintInput.class);

      Urn nodeUrn = Urn.createFromString(input.getNodeUrn());
      if (!nodeUrn.getEntityType().equals("glossaryNode")) {
        throw new DataHubGraphQLException(
            "Provided Urn is not an instance of a glossaryNode",
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
      if (_aspectClient.getAspectOrNull(
              context.getOperationContext(), nodeUrn.toString(), "glossaryNodeKey", 0L)
          == null) {
        throw new DataHubGraphQLException(
            String.format("Failed to create constraint. %s does not exist.", nodeUrn),
            DataHubGraphQLErrorCode.BAD_REQUEST);
      }
      return GraphQLConcurrencyUtils.supplyAsync(
          () -> {
            try {
              // Create the Constraint key.
              final ConstraintKey key = new ConstraintKey();
              key.setId(UUID.randomUUID().toString());

              // Create the constraint info.
              final ConstraintInfo info = new ConstraintInfo();
              info.setDisplayName(input.getName());
              info.setDescription(input.getDescription());
              info.setType(ConstraintType.HAS_GLOSSARY_TERM_IN_NODE.toString());

              ConstraintParams params = new ConstraintParams();
              GlossaryTermInNodeConstraint glossaryNodeConstraint =
                  new GlossaryTermInNodeConstraint();
              glossaryNodeConstraint.setGlossaryNode(Urn.createFromString(input.getNodeUrn()));
              params.setHasGlossaryTermInNodeParams(glossaryNodeConstraint);

              info.setParams(params);

              // Finally, create the MetadataChangeProposal.
              final MetadataChangeProposal proposal =
                  buildMetadataChangeProposalWithKey(
                      key, CONSTRAINT_ENTITY_NAME, CONSTRAINT_INFO_ASPECT_NAME, info);
              return _aspectClient.ingestProposal(context.getOperationContext(), proposal, false);
            } catch (Exception e) {
              throw new RuntimeException("Failed to create constraint", e);
            }
          },
          this.getClass().getSimpleName(),
          "get");
    }
    throw new AuthorizationException(
        "Unauthorized to perform this action. Please contact your DataHub administrator.");
  }
}
