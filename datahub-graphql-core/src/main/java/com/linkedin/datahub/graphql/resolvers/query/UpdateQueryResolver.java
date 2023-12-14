package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.resolvers.ResolverUtils.*;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.UpdateQueryInput;
import com.linkedin.datahub.graphql.types.query.QueryMapper;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjects;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class UpdateQueryResolver implements DataFetcher<CompletableFuture<QueryEntity>> {

  private final QueryService _queryService;

  @Override
  public CompletableFuture<QueryEntity> get(final DataFetchingEnvironment environment)
      throws Exception {

    final QueryContext context = environment.getContext();
    final UpdateQueryInput input =
        bindArgument(environment.getArgument("input"), UpdateQueryInput.class);
    final Urn queryUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(
        () -> {
          final QuerySubjects existingSubjects =
              _queryService.getQuerySubjects(queryUrn, authentication);

          if (existingSubjects == null) {
            // No Query Found
            throw new DataHubGraphQLException(
                String.format("Failed to find query with urn %s", queryUrn),
                DataHubGraphQLErrorCode.NOT_FOUND);
          }

          final List<Urn> subjectUrns =
              existingSubjects.getSubjects().stream()
                  .map(QuerySubject::getEntity)
                  .collect(Collectors.toList());
          final List<Urn> newSubjectUrns =
              input.getSubjects() != null
                  ? input.getSubjects().stream()
                      .map(sub -> UrnUtils.getUrn(sub.getDatasetUrn()))
                      .collect(Collectors.toList())
                  : Collections.emptyList();
          final List<Urn> impactedSubjectUrns = new ArrayList<>();
          impactedSubjectUrns.addAll(subjectUrns);
          impactedSubjectUrns.addAll(newSubjectUrns);

          if (!AuthorizationUtils.canUpdateQuery(impactedSubjectUrns, context)) {
            throw new AuthorizationException(
                "Unauthorized to update Query. Please contact your DataHub administrator if this needs corrective action.");
          }

          try {
            _queryService.updateQuery(
                queryUrn,
                input.getProperties() != null ? input.getProperties().getName() : null,
                input.getProperties() != null ? input.getProperties().getDescription() : null,
                input.getProperties() != null && input.getProperties().getStatement() != null
                    ? new QueryStatement()
                        .setValue(input.getProperties().getStatement().getValue())
                        .setLanguage(
                            QueryLanguage.valueOf(
                                input.getProperties().getStatement().getLanguage().toString()))
                    : null,
                input.getSubjects() != null
                    ? input.getSubjects().stream()
                        .map(
                            sub ->
                                new QuerySubject().setEntity(UrnUtils.getUrn(sub.getDatasetUrn())))
                        .collect(Collectors.toList())
                    : null,
                authentication,
                System.currentTimeMillis());
            return QueryMapper.map(_queryService.getQueryEntityResponse(queryUrn, authentication));
          } catch (Exception e) {
            throw new RuntimeException(
                String.format("Failed to update Query from input %s", input), e);
          }
        });
  }
}
