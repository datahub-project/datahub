package com.linkedin.datahub.graphql.resolvers.query;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.ResourceSpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import graphql.schema.DataFetchingEnvironment;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.testng.Assert.*;


public class DeleteQueryResolverTest {

  private static final Urn TEST_QUERY_URN = UrnUtils.getUrn("urn:li:query:my-unique-query");
  private static final Urn TEST_DATASET_URN = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  @Test
  public void testGetSuccess() throws Exception {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    // User has both required privileges.
    QueryContext mockContext = getMockQueryContext(true, true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteQuery(
        Mockito.eq(TEST_QUERY_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetSuccessCanDeleteQuery() throws Exception {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    QueryContext mockContext = getMockAllowDeleteOnQueryContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteQuery(
        Mockito.eq(TEST_QUERY_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetSuccessCanManageQueries() throws Exception {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    QueryContext mockContext = getMockAllowEditQueriesOnQueryContext();
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertTrue(resolver.get(mockEnv).get());

    Mockito.verify(mockService, Mockito.times(1)).deleteQuery(
        Mockito.eq(TEST_QUERY_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetFailureActorUnauthorized() {
    QueryService mockService = initMockService();
    DeleteQueryResolver resolver = new DeleteQueryResolver(mockService);

    QueryContext mockContext = getMockQueryContext(false, false);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());

    Mockito.verify(mockService, Mockito.times(0)).deleteQuery(
        Mockito.eq(TEST_QUERY_URN),
        Mockito.any(Authentication.class)
    );
  }

  @Test
  public void testGetQueryServiceException() throws Exception {
    // Create resolver
    QueryService mockService = Mockito.mock(QueryService.class);
    Mockito.doThrow(RuntimeException.class).when(mockService).deleteQuery(
        Mockito.any(),
        Mockito.any(Authentication.class));

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
    existingQuerySubjects.setSubjects(new QuerySubjectArray(
        ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))
    ));

    Mockito.when(mockService.getQuerySubjects(
          Mockito.eq(TEST_QUERY_URN),
        Mockito.any(Authentication.class)))
    .thenReturn(existingQuerySubjects);

    return mockService;
  }

  private QueryContext getMockAllowDeleteOnQueryContext() {
    return getMockQueryContext(true, false);
  }

  private QueryContext getMockAllowEditQueriesOnQueryContext() {
    return getMockQueryContext(false, true);
  }

  private QueryContext getMockQueryContext(boolean allowQueryDelete, boolean allowEditEntityQueries) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(DeleteQueryResolverTest.TEST_ACTOR_URN.toString());

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);

    AuthorizationRequest deleteEntityRequest = new AuthorizationRequest(
        DeleteQueryResolverTest.TEST_ACTOR_URN.toString(),
        PoliciesConfig.DELETE_ENTITY_PRIVILEGE.getType(),
        Optional.of(
            new ResourceSpec(
                DeleteQueryResolverTest.TEST_QUERY_URN.getEntityType(),
                DeleteQueryResolverTest.TEST_QUERY_URN.toString()))
    );

    AuthorizationRequest editQueriesRequest = new AuthorizationRequest(
        DeleteQueryResolverTest.TEST_ACTOR_URN.toString(),
        PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
        Optional.of(
            new ResourceSpec(
                DeleteQueryResolverTest.TEST_DATASET_URN.getEntityType(),
                DeleteQueryResolverTest.TEST_DATASET_URN.toString()))
    );

    AuthorizationResult deleteEntityResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(deleteEntityResult.getType()).thenReturn(allowQueryDelete ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(deleteEntityRequest))).thenReturn(deleteEntityResult);

    AuthorizationResult editQueriesResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editQueriesResult.getType()).thenReturn(allowEditEntityQueries ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY);
    Mockito.when(mockAuthorizer.authorize(Mockito.eq(editQueriesRequest))).thenReturn(editQueriesResult);

    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(
        new Authentication(new Actor(ActorType.USER, TEST_ACTOR_URN.getId()), "creds")
    );
    return mockContext;
  }
}
