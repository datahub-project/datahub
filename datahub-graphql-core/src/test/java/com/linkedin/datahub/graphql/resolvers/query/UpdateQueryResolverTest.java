package com.linkedin.datahub.graphql.resolvers.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.QueryLanguage;
import com.linkedin.datahub.graphql.generated.QueryStatementInput;
import com.linkedin.datahub.graphql.generated.UpdateQueryInput;
import com.linkedin.datahub.graphql.generated.UpdateQueryPropertiesInput;
import com.linkedin.datahub.graphql.generated.UpdateQuerySubjectInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.AspectType;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.service.QueryService;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
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

public class UpdateQueryResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_DATASET_URN_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test-2,PROD)");
  private static final Urn TEST_QUERY_URN = UrnUtils.getUrn("urn:li:query:my-unique-query");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final UpdateQueryInput TEST_INPUT =
      new UpdateQueryInput(
          new UpdateQueryPropertiesInput(
              "test-id",
              "test-description",
              new QueryStatementInput("SELECT * FROM TABLE", QueryLanguage.SQL)),
          ImmutableList.of(new UpdateQuerySubjectInput(TEST_DATASET_URN_2.toString())));

  @Test
  public void testGetSuccess() throws Exception {
    // Update resolver
    QueryService mockService = initMockService();
    UpdateQueryResolver resolver = new UpdateQueryResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockQueryContext(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    QueryEntity query = resolver.get(mockEnv).get();
    assertEquals(query.getProperties().getName(), TEST_INPUT.getProperties().getName());
    assertEquals(
        query.getProperties().getDescription(), TEST_INPUT.getProperties().getDescription());
    assertEquals(query.getProperties().getSource().toString(), QuerySource.MANUAL.toString());
    assertEquals(
        query.getProperties().getStatement().getValue(),
        TEST_INPUT.getProperties().getStatement().getValue());
    assertEquals(
        query.getProperties().getStatement().getLanguage(),
        TEST_INPUT.getProperties().getStatement().getLanguage());
    assertEquals(
        query.getSubjects().get(0).getDataset().getUrn(),
        TEST_INPUT.getSubjects().get(0).getDatasetUrn());
    assertEquals(query.getProperties().getCreated().getActor(), TEST_ACTOR_URN.toString());
    assertEquals(query.getProperties().getLastModified().getActor(), TEST_ACTOR_URN.toString());

    Mockito.verify(mockService, Mockito.times(1))
        .updateQuery(
            any(),
            Mockito.eq(TEST_QUERY_URN),
            Mockito.eq(TEST_INPUT.getProperties().getName()),
            Mockito.eq(TEST_INPUT.getProperties().getDescription()),
            Mockito.eq(
                new QueryStatement()
                    .setValue(TEST_INPUT.getProperties().getStatement().getValue())
                    .setLanguage(
                        com.linkedin.query.QueryLanguage.valueOf(
                            TEST_INPUT.getProperties().getStatement().getLanguage().toString()))),
            Mockito.eq(ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN_2))),
            Mockito.anyLong());
  }

  @Test
  public void testGetUnauthorizedNoEditQueriesRights() throws Exception {
    // Update resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryService mockService = Mockito.mock(QueryService.class);
    UpdateQueryResolver resolver = new UpdateQueryResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockQueryContext(false);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetQueryServiceException() throws Exception {
    // Update resolver
    QueryService mockService = Mockito.mock(QueryService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .updateQuery(
            any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyLong());

    UpdateQueryResolver resolver = new UpdateQueryResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockQueryContext(true);
    Mockito.when(mockEnv.getArgument(Mockito.eq("urn"))).thenReturn(TEST_QUERY_URN.toString());
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private QueryService initMockService() {

    // Pre-Update
    QueryService service = Mockito.mock(QueryService.class);

    final QuerySubjects existingSubjects =
        new QuerySubjects()
            .setSubjects(
                new QuerySubjectArray(
                    ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))));

    Mockito.when(service.getQuerySubjects(any(), Mockito.eq(TEST_QUERY_URN)))
        .thenReturn(existingSubjects);

    // Post-Update
    final QueryProperties queryProperties =
        new QueryProperties()
            .setName(TEST_INPUT.getProperties().getName())
            .setDescription(TEST_INPUT.getProperties().getDescription())
            .setCreated(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN))
            .setLastModified(new AuditStamp().setTime(0L).setActor(TEST_ACTOR_URN))
            .setSource(QuerySource.MANUAL)
            .setStatement(
                new QueryStatement()
                    .setValue(TEST_INPUT.getProperties().getStatement().getValue())
                    .setLanguage(
                        com.linkedin.query.QueryLanguage.valueOf(
                            TEST_INPUT.getProperties().getStatement().getLanguage().toString())));

    final QuerySubjects newSubjects =
        new QuerySubjects()
            .setSubjects(
                new QuerySubjectArray(
                    ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN_2))));

    Mockito.when(service.getQueryEntityResponse(any(), Mockito.eq(TEST_QUERY_URN)))
        .thenReturn(
            new EntityResponse()
                .setUrn(TEST_QUERY_URN)
                .setEntityName(Constants.QUERY_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            Constants.QUERY_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.QUERY_PROPERTIES_ASPECT_NAME)
                                .setVersion(0L)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(queryProperties.data())),
                            Constants.QUERY_SUBJECTS_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setName(Constants.QUERY_SUBJECTS_ASPECT_NAME)
                                .setVersion(0L)
                                .setType(AspectType.VERSIONED)
                                .setValue(new Aspect(newSubjects.data()))))));
    return service;
  }

  private QueryContext getMockQueryContext(boolean allowEditEntityQueries) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
    when(mockContext.getOperationContext()).thenReturn(mock(OperationContext.class));

    AuthorizationResult editQueriesResult1 = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editQueriesResult1.getType())
        .thenReturn(
            allowEditEntityQueries
                ? AuthorizationResult.Type.ALLOW
                : AuthorizationResult.Type.DENY);

    AuthorizationResult editAllResult1 = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editAllResult1.getType())
        .thenReturn(
            allowEditEntityQueries
                ? AuthorizationResult.Type.ALLOW
                : AuthorizationResult.Type.DENY);

    AuthorizationResult editQueriesResult2 = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editQueriesResult2.getType())
        .thenReturn(
            allowEditEntityQueries
                ? AuthorizationResult.Type.ALLOW
                : AuthorizationResult.Type.DENY);

    AuthorizationResult editAllResult2 = Mockito.mock(AuthorizationResult.class);
    Mockito.when(editAllResult2.getType())
        .thenReturn(
            allowEditEntityQueries
                ? AuthorizationResult.Type.ALLOW
                : AuthorizationResult.Type.DENY);

    Map<Pair<String, EntitySpec>, AuthorizationResult> responses =
        Map.of(
            Pair.of(
                    PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
                    new EntitySpec(TEST_DATASET_URN.getEntityType(), TEST_DATASET_URN.toString())),
                editQueriesResult1,
            Pair.of(
                    PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType(),
                    new EntitySpec(TEST_DATASET_URN.getEntityType(), TEST_DATASET_URN.toString())),
                editAllResult1,
            Pair.of(
                    PoliciesConfig.EDIT_QUERIES_PRIVILEGE.getType(),
                    new EntitySpec(
                        TEST_DATASET_URN_2.getEntityType(), TEST_DATASET_URN_2.toString())),
                editQueriesResult2,
            Pair.of(
                    PoliciesConfig.EDIT_ENTITY_PRIVILEGE.getType(),
                    new EntitySpec(
                        TEST_DATASET_URN_2.getEntityType(), TEST_DATASET_URN_2.toString())),
                editAllResult2);

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
