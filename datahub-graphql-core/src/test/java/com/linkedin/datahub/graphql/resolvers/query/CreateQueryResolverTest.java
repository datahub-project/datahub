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
import com.linkedin.datahub.graphql.generated.CreateQueryInput;
import com.linkedin.datahub.graphql.generated.CreateQueryPropertiesInput;
import com.linkedin.datahub.graphql.generated.CreateQuerySubjectInput;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.datahub.graphql.generated.QueryLanguage;
import com.linkedin.datahub.graphql.generated.QueryStatementInput;
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

public class CreateQueryResolverTest {

  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:mysql,my-test,PROD)");
  private static final Urn TEST_QUERY_URN = UrnUtils.getUrn("urn:li:query:my-unique-query");
  private static final Urn TEST_ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:test");
  private static final CreateQueryInput TEST_INPUT =
      new CreateQueryInput(
          new CreateQueryPropertiesInput(
              "test-id",
              "test-description",
              new QueryStatementInput("SELECT * FROM TABLE", QueryLanguage.SQL)),
          ImmutableList.of(new CreateQuerySubjectInput(TEST_DATASET_URN.toString())));

  @Test
  public void testGetSuccess() throws Exception {
    // Create resolver
    QueryService mockService = initMockService();
    CreateQueryResolver resolver = new CreateQueryResolver(mockService);

    // Execute resolver
    QueryContext mockContext = getMockQueryContext(true);
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
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
        .createQuery(
            any(),
            Mockito.eq(TEST_INPUT.getProperties().getName()),
            Mockito.eq(TEST_INPUT.getProperties().getDescription()),
            Mockito.eq(QuerySource.MANUAL),
            Mockito.eq(
                new QueryStatement()
                    .setValue(TEST_INPUT.getProperties().getStatement().getValue())
                    .setLanguage(
                        com.linkedin.query.QueryLanguage.valueOf(
                            TEST_INPUT.getProperties().getStatement().getLanguage().toString()))),
            Mockito.eq(ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))),
            Mockito.anyLong());
  }

  @Test
  public void testGetUnauthorized() throws Exception {
    // Create resolver
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    QueryService mockService = Mockito.mock(QueryService.class);
    CreateQueryResolver resolver = new CreateQueryResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockQueryContext(false);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
    Mockito.verify(mockClient, Mockito.times(0)).ingestProposal(any(), Mockito.any(), anyBoolean());
  }

  @Test
  public void testGetQueryServiceException() throws Exception {
    // Create resolver
    QueryService mockService = Mockito.mock(QueryService.class);
    Mockito.doThrow(RuntimeException.class)
        .when(mockService)
        .createQuery(
            any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.any(),
            Mockito.anyLong());

    CreateQueryResolver resolver = new CreateQueryResolver(mockService);

    // Execute resolver
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    QueryContext mockContext = getMockQueryContext(true);
    Mockito.when(mockEnv.getArgument(Mockito.eq("input"))).thenReturn(TEST_INPUT);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    assertThrows(CompletionException.class, () -> resolver.get(mockEnv).join());
  }

  private QueryService initMockService() {
    QueryService service = Mockito.mock(QueryService.class);
    Mockito.when(
            service.createQuery(
                any(),
                Mockito.eq(TEST_INPUT.getProperties().getName()),
                Mockito.eq(TEST_INPUT.getProperties().getDescription()),
                Mockito.eq(QuerySource.MANUAL),
                Mockito.eq(
                    new QueryStatement()
                        .setValue(TEST_INPUT.getProperties().getStatement().getValue())
                        .setLanguage(
                            com.linkedin.query.QueryLanguage.valueOf(
                                TEST_INPUT
                                    .getProperties()
                                    .getStatement()
                                    .getLanguage()
                                    .toString()))),
                Mockito.eq(ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))),
                Mockito.anyLong()))
        .thenReturn(TEST_QUERY_URN);

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

    final QuerySubjects querySubjects =
        new QuerySubjects()
            .setSubjects(
                new QuerySubjectArray(
                    ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))));

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
                                .setValue(new Aspect(querySubjects.data()))))));
    return service;
  }

  private QueryContext getMockQueryContext(boolean allowEditEntityQueries) {
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN.toString());
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
