package com.linkedin.datahub.graphql.types.query;

import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
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
import com.linkedin.query.QueryLanguage;
import com.linkedin.query.QueryProperties;
import com.linkedin.query.QuerySource;
import com.linkedin.query.QueryStatement;
import com.linkedin.query.QuerySubject;
import com.linkedin.query.QuerySubjectArray;
import com.linkedin.query.QuerySubjects;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
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
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(queryUrn1, queryUrn2))),
                Mockito.eq(com.linkedin.datahub.graphql.types.query.QueryType.ASPECTS_TO_FETCH),
                Mockito.any(Authentication.class)))
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
    List<DataFetcherResult<QueryEntity>> result =
        type.batchLoad(
            ImmutableList.of(TEST_QUERY_URN.toString(), TEST_QUERY_2_URN.toString()), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            Mockito.eq(Constants.QUERY_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(queryUrn1, queryUrn2)),
            Mockito.eq(QueryType.ASPECTS_TO_FETCH),
            Mockito.any(Authentication.class));

    assertEquals(result.size(), 2);

    QueryEntity query1 = result.get(0).getData();
    verifyQuery1(query1);

    QueryEntity query2 = result.get(1).getData();
    verifyQuery2(query2);
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
                Mockito.eq(Constants.QUERY_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(queryUrn1, queryUrn2))),
                Mockito.eq(com.linkedin.datahub.graphql.types.query.QueryType.ASPECTS_TO_FETCH),
                Mockito.any(Authentication.class)))
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
    List<DataFetcherResult<QueryEntity>> result =
        type.batchLoad(
            ImmutableList.of(TEST_QUERY_URN.toString(), TEST_QUERY_2_URN.toString()), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            Mockito.eq(Constants.QUERY_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(queryUrn1, queryUrn2)),
            Mockito.eq(QueryType.ASPECTS_TO_FETCH),
            Mockito.any(Authentication.class));

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
        .batchGetV2(
            Mockito.anyString(),
            Mockito.anySet(),
            Mockito.anySet(),
            Mockito.any(Authentication.class));
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
