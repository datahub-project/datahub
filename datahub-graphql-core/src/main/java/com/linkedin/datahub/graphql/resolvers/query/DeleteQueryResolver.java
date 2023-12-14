package com.linkedin.datahub.graphql.resolvers.query;

import com.datahub.authentication.Authentication;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjects;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeleteQueryResolver implements DataFetcher<CompletableFuture<Boolean>> {

  private final QueryService _queryService;

  @Override
  public CompletableFuture<Boolean> get(final DataFetchingEnvironment environment)
      throws Exception {
    final QueryContext context = environment.getContext();
    final Urn queryUrn = UrnUtils.getUrn(environment.getArgument("urn"));
    final Authentication authentication = context.getAuthentication();

    return CompletableFuture.supplyAsync(
        () -> {
          final QuerySubjects existingSubjects =
              _queryService.getQuerySubjects(queryUrn, authentication);
          final List<Urn> subjectUrns =
              existingSubjects != null
                  ? existingSubjects.getSubjects().stream()
                      .map(QuerySubject::getEntity)
                      .collect(Collectors.toList())
                  : Collections.emptyList();

          if (!AuthorizationUtils.canDeleteQuery(queryUrn, subjectUrns, context)) {
            throw new AuthorizationException(
                "Unauthorized to delete Query. Please contact your DataHub administrator if this needs corrective action.");
          }

          try {
            _queryService.deleteQuery(queryUrn, authentication);
            return true;
          } catch (Exception e) {
            throw new RuntimeException("Failed to delete Query", e);
          }
        });
  }
}
