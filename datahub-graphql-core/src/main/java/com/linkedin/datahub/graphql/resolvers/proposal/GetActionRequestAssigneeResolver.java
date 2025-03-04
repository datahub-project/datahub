package com.linkedin.datahub.graphql.resolvers.proposal;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetActionRequestAssigneeInput;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.service.ActionRequestService;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class GetActionRequestAssigneeResolver
    implements DataFetcher<CompletableFuture<List<String>>> {

  private final ActionRequestService actionRequestService;

  @Override
  public CompletableFuture<List<String>> get(DataFetchingEnvironment environment) throws Exception {
    final QueryContext context = environment.getContext();

    final GetActionRequestAssigneeInput input =
        bindArgument(environment.getArgument("input"), GetActionRequestAssigneeInput.class);

    return CompletableFuture.supplyAsync(
        () -> {
          List<String> actors;
          SubResourceType subResourceType = null;
          if (input.getSubResourceType() != null) {
            subResourceType = SubResourceType.valueOf(input.getSubResourceType());
          }
          try {
            actors =
                this.actionRequestService.getAssignee(
                    context.getOperationContext(),
                    input.getActionRequestType(),
                    Urn.createFromString(input.getResourceUrn()),
                    null,
                    subResourceType);
          } catch (URISyntaxException e) {
            throw new RuntimeException(e);
          }
          return actors;
        });
  }
}
