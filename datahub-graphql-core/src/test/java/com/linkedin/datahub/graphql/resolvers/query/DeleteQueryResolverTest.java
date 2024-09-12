package com.linkedin.datahub.graphql.resolvers.query;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import com.linkedin.util.Pair;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DeleteQueryResolverTest {

  private static final Urn TEST_QUERY_URN = UrnUtils.getUrn("urn:li:query:my-unique-query");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testGetSuccess() throws Exception {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    // User has both required privileges.
    QueryContext mockContext = getMockQueryContext(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteQuery(any(), Mockito.eq(TEST_QUERY_URN));
  }

  @Test
  public void testGetSuccessCanEditQueries() throws Exception {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    QueryContext mockContext = getMockAllowEditQueriesOnQueryContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteQuery(any(), Mockito.eq(TEST_QUERY_URN));
  }

  @Test
  public void testGetFailureActorUnauthorized() {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    QueryContext mockContext = getMockQueryContext(false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockService, Mockito.times(0)).deleteQuery(any(), Mockito.eq(TEST_QUERY_URN));
  }

  @Test
  public void testGetQueryServiceException() throws Exception {
    // Create resolver
    QueryService mockService = Mockito.mock(QueryService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).deleteQuery(any(), Mockito.any());

    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private static QueryService initMockService() {
    QueryService mockService = Mockito.mock(QueryService.class);

    QuerySubjects existingQuerySubjects = new QuerySubjects();
    existingQuerySubjects.setSubjects(
        new QuerySubjectArray(ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))));

    Mockito.when(mockService.getQuerySubjects(any(), Mockito.eq(TEST_QUERY_URN)))
        .thenReturn(existingQuerySubjects);

    return mockService;
  }

  private QueryContext getMockAllowEditQueriesOnQueryContext() {
    return getMockQueryContext(true);
  }

  private QueryContext getMockQueryContext(boolean allowEditEntityQueries) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn())
        .thenReturn(DeleteQueryResolverTest.TEST_ACTOR_URN.toString());
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    AuthorizationResult editQueriesResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editQueriesResult.getType())
        .thenReturn(
            allowEditEntityQueries
                ? AuthorizationResult.Type.ALLOW
                : AuthorizationResult.Type.DENY);

    AuthorizationResult editAllResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editAllResult.getType())
        .thenReturn(
            allowEditEntityQueries
                ? AuthorizationResult.Type.ALLOW
                : AuthorizationResult.Type.DENY);

    Map<Pair<String, EntitySpec>, AuthorizationResult> responses =
        Map.of(
            Pair.of(
                    PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
                    new EntitySpec(TEST_DATASET_URN.getEntityType(), TEST_DATASET_URN.toString())),
                editQueriesResult,
            Pair.of(
                    PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType(),
                    new EntitySpec(TEST_DATASET_URN.getEntityType(), TEST_DATASET_URN.toString())),
                editAllResult);

    when(mockContext.getOperationContext().authorize(any(), any()))
        .thenAnswer(
            args ->
                responses.getOrDefault(
                    Pair.of(args.getArgument(0), args.getArgument(1)),
                    new AuthorizationResult(null, AuthorizationResult.Type.DENY, "")));

    Mockito.when(mockContext.getAuthentication())
        .thenReturn(new Authentication(new Actor(ActorType.USER, TEST_ACTOR_URN.getId()), "creds"));
    return mockContext;
  }
}
