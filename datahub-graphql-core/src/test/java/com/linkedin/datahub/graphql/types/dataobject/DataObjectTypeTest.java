package com.linkedin.datahub.graphql.types.dataobject;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.application.Applications;
import com.linkedin.common.FabricType;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataObject;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.dataobject.DataObjectProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.DataObjectKey;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import java.util.HashSet;
import java.util.List;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DataObjectTypeTest {

  private static final String TEST_DATA_OBJECT_1_URN =
      "urn:li:dataObject:(urn:li:dataPlatform:s3,b/clip.mp4,PROD)";
  private static final String TEST_PLATFORM_URN = "urn:li:dataPlatform:s3";
  private static final String TEST_APPLICATION_URN = "urn:li:application:test-app";
  private static final String TEST_DATA_OBJECT_2_URN =
      "urn:li:dataObject:(urn:li:dataPlatform:s3,b/missing,PROD)";

  private static EnvelopedAspectMap dataObject1Aspects() throws Exception {
    final DataObjectKey key = new DataObjectKey();
    key.setPlatform(Urn.createFromString(TEST_PLATFORM_URN));
    key.setName("b/clip.mp4");
    key.setOrigin(FabricType.PROD);

    final DataObjectProperties properties = new DataObjectProperties();
    properties.setName("clip.mp4");
    properties.setQualifiedName("b/clip.mp4");

    final Applications applications = new Applications();
    applications.setApplications(
        new UrnArray(ImmutableList.of(Urn.createFromString(TEST_APPLICATION_URN))));

    return new EnvelopedAspectMap(
        ImmutableMap.of(
            Constants.DATA_OBJECT_KEY_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(key.data())),
            Constants.DATA_OBJECT_PROPERTIES_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(properties.data())),
            Constants.APPLICATION_MEMBERSHIP_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(applications.data()))));
  }

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn dataObjectUrn1 = Urn.createFromString(TEST_DATA_OBJECT_1_URN);
    Urn dataObjectUrn2 = Urn.createFromString(TEST_DATA_OBJECT_2_URN);

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(Constants.DATA_OBJECT_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(dataObjectUrn1, dataObjectUrn2))),
                Mockito.eq(DataObjectType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                dataObjectUrn1,
                new EntityResponse()
                    .setEntityName(Constants.DATA_OBJECT_ENTITY_NAME)
                    .setUrn(dataObjectUrn1)
                    .setAspects(dataObject1Aspects())));

    DataObjectType type = new DataObjectType(client);

    QueryContext mockContext = getMockAllowContext();
    List<DataFetcherResult<DataObject>> result =
        type.batchLoad(
            ImmutableList.of(TEST_DATA_OBJECT_1_URN, TEST_DATA_OBJECT_2_URN), mockContext);

    // Lock the fetch set: this asserts batchGetV2 is invoked with exactly ASPECTS_TO_FETCH, so a
    // future aspect that is fetched but never mapped (or a capability aspect dropped from the fetch
    // set, as `applications` once was) becomes a conscious, test-breaking change.
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(Constants.DATA_OBJECT_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(dataObjectUrn1, dataObjectUrn2)),
            Mockito.eq(DataObjectType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    DataObject dataObject1 = result.get(0).getData();
    assertEquals(dataObject1.getUrn(), TEST_DATA_OBJECT_1_URN);
    assertEquals(dataObject1.getType(), EntityType.DATA_OBJECT);

    assertNotNull(dataObject1.getProperties());
    assertEquals(dataObject1.getProperties().getName(), "clip.mp4");

    // The application membership capability must survive batch load -> map. This is the assertion
    // that would have caught the original gap where `applications` was declared but never fetched.
    assertNotNull(dataObject1.getApplications());
    assertEquals(dataObject1.getApplications().size(), 1);
    assertEquals(
        dataObject1.getApplications().get(0).getApplication().getUrn(), TEST_APPLICATION_URN);

    // Unmatched urn returns a null entry (the gmsResult == null branch).
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    DataObjectType type = new DataObjectType(mockClient);

    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () ->
            type.batchLoad(
                ImmutableList.of(TEST_DATA_OBJECT_1_URN, TEST_DATA_OBJECT_2_URN), context));
  }

  @Test
  public void testEntityType() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataObjectType type = new DataObjectType(mockClient);

    assertEquals(type.type(), EntityType.DATA_OBJECT);
  }

  @Test
  public void testObjectClass() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataObjectType type = new DataObjectType(mockClient);

    assertEquals(type.objectClass(), DataObject.class);
  }

  @Test
  public void testAutoComplete() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);

    com.linkedin.metadata.query.AutoCompleteResult mockResult =
        new com.linkedin.metadata.query.AutoCompleteResult();
    mockResult.setQuery("test");
    mockResult.setSuggestions(new com.linkedin.data.template.StringArray());
    mockResult.setEntities(new com.linkedin.metadata.query.AutoCompleteEntityArray());

    Mockito.when(
            mockClient.autoComplete(
                any(),
                Mockito.eq(Constants.DATA_OBJECT_ENTITY_NAME),
                Mockito.eq("test"),
                Mockito.any(),
                Mockito.eq(10)))
        .thenReturn(mockResult);

    DataObjectType type = new DataObjectType(mockClient);
    QueryContext context = getMockAllowContext();

    com.linkedin.datahub.graphql.generated.AutoCompleteResults result =
        type.autoComplete("test", null, null, 10, context);

    assertNotNull(result);
    assertEquals(result.getQuery(), "test");
    Mockito.verify(mockClient, Mockito.times(1))
        .autoComplete(
            any(),
            Mockito.eq(Constants.DATA_OBJECT_ENTITY_NAME),
            Mockito.eq("test"),
            Mockito.any(),
            Mockito.eq(10));
  }

  @Test(expectedExceptions = org.apache.commons.lang3.NotImplementedException.class)
  public void testSearchThrowsNotImplementedException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataObjectType type = new DataObjectType(mockClient);
    QueryContext context = getMockAllowContext();

    type.search("test query", null, 0, 10, context);
  }

  @Test
  public void testGetKeyProvider() {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    DataObjectType type = new DataObjectType(mockClient);

    DataObject dataObject = new DataObject();
    dataObject.setUrn(TEST_DATA_OBJECT_1_URN);

    String key = type.getKeyProvider().apply(dataObject);
    assertEquals(key, TEST_DATA_OBJECT_1_URN);
  }
}
