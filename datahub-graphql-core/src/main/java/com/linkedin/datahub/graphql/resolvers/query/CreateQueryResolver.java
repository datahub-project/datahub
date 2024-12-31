package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.concurrency.GraphQLConcurrencyUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateQueryInput;
import com.linkedin.datahub.graphql.generated.CreateQuerySubjectInput;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.types.query.QueryMapper;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class CreateQueryResolver implements DataFetcher<CompletableFuture<QueryEntity>> {

  private final QueryService _queryService;

  @Override
  public CompletableFuture<QueryEntity> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final CreateQueryInput input =
        bindArgument(environment.getArgument("input"), CreateQueryInput.class);
    final Authentication authentication = context.getAuthentication();

    return GraphQLConcurrencyUtils.supplyAsync(
        () -> {
          if (!AuthorizationUtils.canCreateQuery(
              input.getSubjects().stream()
                  .map(CreateQuerySubjectInput::getDatasetUrn)
                  .map(UrnUtils::getUrn)
                  .collect(Collectors.toList()),
              context)) {
            throw new AuthorizationException(
                "Unauthorized to create Query. Please contact your DataHub administrator for more information.");
          }

          try {
            final Urn queryUrn =
                _queryService.createQuery(
                    context.getOperationContext(),
                    input.getProperties().getName(),
                    input.getProperties().getDescription(),
                    QuerySource.MANUAL,
                    new QueryStatement()
                        .setValue(input.getProperties().getStatement().getValue())
                        .setLanguage(
                            QueryLanguage.valueOf(
                                input.getProperties().getStatement().getLanguage().toString())),
                    input.getSubjects().stream()
                        .map(
                            sub ->
                                new QuerySubject().setEntity(UrnUtils.getUrn(sub.getDatasetUrn())))
                        .collect(Collectors.toList()),
                    System.currentTimeMillis());
            return QueryMapper.map(
                context,
                _queryService.getQueryEntityResponse(context.getOperationContext(), queryUrn));
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to create a new Query from input %s", input), e);
          }
        },
        this.getClass().getSimpleName(),
        "get");
  }
}
