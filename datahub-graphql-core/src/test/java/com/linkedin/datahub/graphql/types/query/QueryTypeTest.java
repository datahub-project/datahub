package com.linkedin.datahub.graphql.types.query;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.QueryEntity;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.OperationContextConfig;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class QueryTypeTest {

  private static final Urn TEST_QUERY_URN = UrnUtils.getUrn("urn:li:query:test");
  private static final Urn TEST_QUERY_2_URN = UrnUtils.getUrn("urn:li:query:test-2");
  private static final Urn TEST_DATASET_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
  private static final Urn TEST_DATASET_2_URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test-2,PROD)");
  private static final Urn TEST_USER_URN = UrnUtils.getUrn("urn:li:corpuser:test");

  private static final QueryProperties TEST_QUERY_PROPERTIES_1 =
      new QueryProperties()
          .setName("Query Name")
          .setDescription("Query Description")
          .setSource(QuerySource.MANUAL)
          .setStatement(
              new QueryStatement()
                  .setLanguage(QueryLanguage.SQL)
                  .setValue("SELECT * FROM MyTestTable"))
          .setCreated(new AuditStamp().setActor(TEST_USER_URN).setTime(0L))
          .setLastModified(new AuditStamp().setActor(TEST_USER_URN).setTime(1L));
  private static final QuerySubjects TEST_QUERY_SUBJECTS_1 =
      new QuerySubjects()
          .setSubjects(
              new QuerySubjectArray(
                  ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_URN))));
  private static final QueryProperties TEST_QUERY_PROPERTIES_2 =
      new QueryProperties()
          .setName("Query Name 2")
          .setDescription("Query Description 2")
          .setSource(QuerySource.MANUAL)
          .setStatement(
              new QueryStatement()
                  .setLanguage(QueryLanguage.SQL)
                  .setValue("SELECT * FROM MyTestTable2"))
          .setCreated(new AuditStamp().setActor(TEST_USER_URN).setTime(0L))
          .setLastModified(new AuditStamp().setActor(TEST_USER_URN).setTime(1L));
  private static final QuerySubjects TEST_QUERY_SUBJECTS_2 =
      new QuerySubjects()
          .setSubjects(
              new QuerySubjectArray(
                  ImmutableList.of(new QuerySubject().setEntity(TEST_DATASET_2_URN))));

  @Test
  public void testBatchLoad() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn queryUrn1 = TEST_QUERY_URN;
    Urn queryUrn2 = TEST_QUERY_2_URN;

    Map<String, EnvelopedAspect> query1Aspects = new HashMap<>();
    query1Aspects.put(
        Constants.QUERY_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_PROPERTIES_1.data())));
    query1Aspects.put(
        Constants.QUERY_SUBJECTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_SUBJECTS_1.data())));

    Map<String, EnvelopedAspect> query2Aspects = new HashMap<>();
    query2Aspects.put(
        Constants.QUERY_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_PROPERTIES_2.data())));
    query2Aspects.put(
        Constants.QUERY_SUBJECTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_SUBJECTS_2.data())));

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(queryUrn1, queryUrn2))),
                Mockito.eq(com.linkedin.datahub.graphql.types.query.QueryType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                queryUrn1,
                new EntityResponse()
                    .setEntityName(Constants.QUERY_ENTITY_NAME)
                    .setUrn(queryUrn1)
                    .setAspects(new EnvelopedAspectMap(query1Aspects)),
                queryUrn2,
                new EntityResponse()
                    .setEntityName(Constants.QUERY_ENTITY_NAME)
                    .setUrn(queryUrn2)
                    .setAspects(new EnvelopedAspectMap(query2Aspects))));

    QueryType type = new QueryType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<QueryEntity>> result =
        type.batchLoad(
            ImmutableList.of(TEST_QUERY_URN.toString(), TEST_QUERY_2_URN.toString()), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.QUERY_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(queryUrn1, queryUrn2)),
            Mockito.eq(QueryType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    QueryEntity query1 = result.get(0).getData();
    verifyQuery1(query1);

    QueryEntity query2 = result.get(1).getData();
    verifyQuery2(query2);
  }

  @Test
  public void testBatchLoadWithViewAuthorizationEnabled_filtersUnauthorizedQueries()
      throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Map<String, EnvelopedAspect> queryAspects = new HashMap<>();
    queryAspects.put(
        Constants.QUERY_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_PROPERTIES_1.data())));
    queryAspects.put(
        Constants.QUERY_SUBJECTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_SUBJECTS_1.data())));

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(TEST_QUERY_URN))),
                Mockito.eq(QueryType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                TEST_QUERY_URN,
                new EntityResponse()
                    .setEntityName(Constants.QUERY_ENTITY_NAME)
                    .setUrn(TEST_QUERY_URN)
                    .setAspects(new EnvelopedAspectMap(queryAspects))));

    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    Mockito.when(mockAuthorizer.authorize(any(AuthorizationRequest.class)))
        .thenReturn(new AuthorizationResult(null, AuthorizationResult.Type.DENY, ""));

    AspectRetriever aspectRetriever = Mockito.mock(AspectRetriever.class);
    Mockito.when(aspectRetriever.getEntityRegistry())
        .thenReturn(TestOperationContexts.defaultEntityRegistry());
    Mockito.when(
            aspectRetriever.getLatestAspectObjects(
                any(),
                eq(ImmutableSet.of(TEST_QUERY_URN)),
                eq(ImmutableSet.of(Constants.QUERY_SUBJECTS_ASPECT_NAME))))
        .thenReturn(
            ImmutableMap.of(
                TEST_QUERY_URN,
                ImmutableMap.of(
                    Constants.QUERY_SUBJECTS_ASPECT_NAME,
                    new Aspect(TEST_QUERY_SUBJECTS_1.data()))));

    QueryType type = new QueryType(client);
    OperationContext userContext = createUserContextWithViewAuth(mockAuthorizer, aspectRetriever);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext()).thenReturn(userContext);

    List<DataFetcherResult<QueryEntity>> result =
        type.batchLoad(ImmutableList.of(TEST_QUERY_URN.toString()), mockContext);

    assertEquals(result.size(), 1);
    assertNull(result.get(0));
  }

  @Test
  public void testBatchLoadNullEntity() throws Exception {

    EntityClient client = Mockito.mock(EntityClient.class);

    Urn queryUrn1 = TEST_QUERY_URN;
    Urn queryUrn2 = TEST_QUERY_2_URN;

    Map<String, EnvelopedAspect> query1Aspects = new HashMap<>();
    query1Aspects.put(
        Constants.QUERY_PROPERTIES_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_PROPERTIES_1.data())));
    query1Aspects.put(
        Constants.QUERY_SUBJECTS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_QUERY_SUBJECTS_1.data())));
    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(queryUrn1, queryUrn2))),
                Mockito.eq(com.linkedin.datahub.graphql.types.query.QueryType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                queryUrn1,
                new EntityResponse()
                    .setEntityName(Constants.QUERY_ENTITY_NAME)
                    .setUrn(queryUrn1)
                    .setAspects(new EnvelopedAspectMap(query1Aspects))));

    QueryType type = new QueryType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<QueryEntity>> result =
        type.batchLoad(
            ImmutableList.of(TEST_QUERY_URN.toString(), TEST_QUERY_2_URN.toString()), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.QUERY_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(queryUrn1, queryUrn2)),
            Mockito.eq(QueryType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    QueryEntity query = result.get(0).getData();
    verifyQuery1(query);

    // Assert second element is null (we didn't return it)
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    QueryType type = new QueryType(mockClient);

    // Execute Batch load
    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () ->
            type.batchLoad(
                ImmutableList.of(TEST_QUERY_URN.toString(), TEST_QUERY_2_URN.toString()), context));
  }

  private OperationContext createUserContextWithViewAuth(
      Authorizer authorizer, AspectRetriever aspectRetriever) {
    Authentication userAuth =
        new Authentication(new Actor(ActorType.USER, TEST_USER_URN.getId()), "");

    RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .aspectRetriever(aspectRetriever)
            .cachingAspectRetriever(
                TestOperationContexts.emptyActiveUsersAspectRetriever(
                    aspectRetriever::getEntityRegistry))
            .graphRetriever(GraphRetriever.EMPTY)
            .searchRetriever(SearchRetriever.EMPTY)
            .build();

    OperationContext systemContext =
        TestOperationContexts.systemContext(
            () ->
                OperationContextConfig.builder()
                    .viewAuthorizationConfiguration(
                        ViewAuthorizationConfiguration.builder().enabled(true).build())
                    .build(),
            null,
            null,
            null,
            () -> retrieverContext,
            null,
            null,
            null);

    return systemContext.asSession(RequestContext.TEST, authorizer, userAuth);
  }

  private void verifyQuery1(QueryEntity query) {
    assertEquals(query.getUrn(), TEST_QUERY_URN.toString());
    assertEquals(query.getType(), EntityType.QUERY);
    assertEquals(query.getProperties().getName(), TEST_QUERY_PROPERTIES_1.getName());
    assertEquals(query.getProperties().getDescription(), TEST_QUERY_PROPERTIES_1.getDescription());
    assertEquals(
        query.getProperties().getSource().toString(),
        TEST_QUERY_PROPERTIES_1.getSource().toString());
    assertEquals(
        query.getProperties().getStatement().getLanguage().toString(),
        TEST_QUERY_PROPERTIES_1.getStatement().getLanguage().toString());
    assertEquals(
        query.getProperties().getStatement().getValue(),
        TEST_QUERY_PROPERTIES_1.getStatement().getValue());
    assertEquals(
        query.getProperties().getCreated().getActor(),
        TEST_QUERY_PROPERTIES_1.getCreated().getActor().toString());
    assertEquals(
        query.getProperties().getCreated().getTime(),
        TEST_QUERY_PROPERTIES_1.getCreated().getTime());
    assertEquals(
        query.getProperties().getLastModified().getActor(),
        TEST_QUERY_PROPERTIES_1.getLastModified().getActor().toString());
    assertEquals(
        query.getProperties().getLastModified().getTime(),
        TEST_QUERY_PROPERTIES_1.getLastModified().getTime());
    assertEquals(
        query.getSubjects().get(0).getDataset().getUrn(),
        TEST_QUERY_SUBJECTS_1.getSubjects().get(0).getEntity().toString());
  }

  private void verifyQuery2(QueryEntity query) {
    assertEquals(query.getUrn(), TEST_QUERY_2_URN.toString());
    assertEquals(query.getType(), EntityType.QUERY);
    assertEquals(query.getProperties().getName(), TEST_QUERY_PROPERTIES_2.getName());
    assertEquals(query.getProperties().getDescription(), TEST_QUERY_PROPERTIES_2.getDescription());
    assertEquals(
        query.getProperties().getSource().toString(),
        TEST_QUERY_PROPERTIES_2.getSource().toString());
    assertEquals(
        query.getProperties().getStatement().getLanguage().toString(),
        TEST_QUERY_PROPERTIES_2.getStatement().getLanguage().toString());
    assertEquals(
        query.getProperties().getStatement().getValue(),
        TEST_QUERY_PROPERTIES_2.getStatement().getValue());
    assertEquals(
        query.getProperties().getCreated().getActor(),
        TEST_QUERY_PROPERTIES_2.getCreated().getActor().toString());
    assertEquals(
        query.getProperties().getCreated().getTime(),
        TEST_QUERY_PROPERTIES_2.getCreated().getTime());
    assertEquals(
        query.getProperties().getLastModified().getActor(),
        TEST_QUERY_PROPERTIES_2.getLastModified().getActor().toString());
    assertEquals(
        query.getProperties().getLastModified().getTime(),
        TEST_QUERY_PROPERTIES_2.getLastModified().getTime());
    assertEquals(
        query.getSubjects().get(0).getDataset().getUrn(),
        TEST_QUERY_SUBJECTS_2.getSubjects().get(0).getEntity().toString());
  }
}
