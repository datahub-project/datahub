package com.linkedin.datahub.graphql.types.test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Test;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;

public class TestTypeTest {

  @Mock private EntityClient mockEntityClient;

  @Mock private QueryContext mockQueryContext;

  @Mock private AspectRetriever mockAspectRetriever;

  @Mock private EntityRegistry mockEntityRegistry;

  @Mock private OperationContext operationContext;

  private TestType testType;
  private Urn testUrn1;
  private Urn testUrn2;
  private Urn testUrn3;

  @BeforeMethod
  public void setup() throws URISyntaxException {
    MockitoAnnotations.openMocks(this);
    testType = new TestType(mockEntityClient);

    testUrn1 = new Urn("urn:li:test:test1");
    testUrn2 = new Urn("urn:li:test:test2");
    testUrn3 = new Urn("urn:li:test:test3");

    when(mockQueryContext.getOperationContext()).thenReturn(operationContext);
    when(operationContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(operationContext.getAspectRetriever()).thenReturn(mockAspectRetriever);
  }

  @org.testng.annotations.Test
  public void testType() {
    assertEquals(testType.type(), EntityType.TEST);
  }

  @org.testng.annotations.Test
  public void testObjectClass() {
    assertEquals(testType.objectClass(), Test.class);
  }

  @org.testng.annotations.Test
  public void testGetKeyProvider() {
    Entity mockEntity = mock(Entity.class);
    when(mockEntity.getUrn()).thenReturn(testUrn1.toString());

    String result = testType.getKeyProvider().apply(mockEntity);
    assertEquals(result, testUrn1.toString());
  }

  @org.testng.annotations.Test
  public void testBatchLoadSuccess() throws Exception {
    // Setup
    List<String> urns = Arrays.asList(testUrn1.toString(), testUrn2.toString());

    EntityResponse entityResponse1 = createMockEntityResponse(testUrn1, true);
    EntityResponse entityResponse2 = createMockEntityResponse(testUrn2, true);

    Map<Urn, EntityResponse> entityResponseMap =
        ImmutableMap.of(
            testUrn1, entityResponse1,
            testUrn2, entityResponse2);

    when(mockEntityClient.batchGetV2(
            eq(mockQueryContext.getOperationContext()),
            eq(Constants.TEST_ENTITY_NAME),
            any(HashSet.class),
            eq(TestType.ASPECTS_TO_FETCH)))
        .thenReturn(entityResponseMap);

    // Mock TestMapper
    try (MockedStatic<TestMapper> testMapperMock = mockStatic(TestMapper.class)) {
      Test mockTest1 = createMockTest(testUrn1.toString());
      Test mockTest2 = createMockTest(testUrn2.toString());

      testMapperMock.when(() -> TestMapper.map(entityResponse1)).thenReturn(mockTest1);
      testMapperMock.when(() -> TestMapper.map(entityResponse2)).thenReturn(mockTest2);

      // Execute
      List<DataFetcherResult<Test>> results = testType.batchLoad(urns, mockQueryContext);

      // Verify
      assertEquals(results.size(), 2);
      assertEquals(results.get(0).getData().getUrn(), testUrn1.toString());
      assertEquals(results.get(1).getData().getUrn(), testUrn2.toString());
    }
  }

  @org.testng.annotations.Test
  public void testBatchLoadWithNullGmsResult() throws Exception {
    // Setup - one entity exists, one doesn't
    List<String> urns = Arrays.asList(testUrn1.toString(), testUrn2.toString());

    EntityResponse entityResponse1 = createMockEntityResponse(testUrn1, true);

    Map<Urn, EntityResponse> entityResponseMap =
        ImmutableMap.of(
            testUrn1, entityResponse1
            // testUrn2 is intentionally missing to simulate null gmsResult
            );

    when(mockEntityClient.batchGetV2(
            eq(mockQueryContext.getOperationContext()),
            eq(Constants.TEST_ENTITY_NAME),
            any(HashSet.class),
            eq(TestType.ASPECTS_TO_FETCH)))
        .thenReturn(entityResponseMap);

    // Mock TestMapper
    try (MockedStatic<TestMapper> testMapperMock = mockStatic(TestMapper.class)) {
      Test mockTest1 = createMockTest(testUrn1.toString());

      testMapperMock.when(() -> TestMapper.map(entityResponse1)).thenReturn(mockTest1);

      // Execute
      List<DataFetcherResult<Test>> results = testType.batchLoad(urns, mockQueryContext);

      // Verify - only one result should be returned (null result filtered out)
      assertEquals(results.size(), 1);
      assertEquals(results.get(0).getData().getUrn(), testUrn1.toString());
    }
  }

  @org.testng.annotations.Test
  public void testBatchLoadWithTestMapperReturningNull() throws Exception {
    // Setup - entity exists but TestMapper returns null (missing test info)
    List<String> urns = Arrays.asList(testUrn1.toString(), testUrn2.toString());

    EntityResponse entityResponse1 = createMockEntityResponse(testUrn1, true);
    EntityResponse entityResponse2 = createMockEntityResponse(testUrn2, false); // No test info

    Map<Urn, EntityResponse> entityResponseMap =
        ImmutableMap.of(
            testUrn1, entityResponse1,
            testUrn2, entityResponse2);

    when(mockEntityClient.batchGetV2(
            eq(mockQueryContext.getOperationContext()),
            eq(Constants.TEST_ENTITY_NAME),
            any(HashSet.class),
            eq(TestType.ASPECTS_TO_FETCH)))
        .thenReturn(entityResponseMap);

    // Mock TestMapper
    try (MockedStatic<TestMapper> testMapperMock = mockStatic(TestMapper.class)) {
      Test mockTest1 = createMockTest(testUrn1.toString());

      testMapperMock.when(() -> TestMapper.map(entityResponse1)).thenReturn(mockTest1);
      testMapperMock
          .when(() -> TestMapper.map(entityResponse2))
          .thenReturn(null); // TestMapper returns null

      // Execute
      List<DataFetcherResult<Test>> results = testType.batchLoad(urns, mockQueryContext);

      // Verify - only one result should be returned (null result filtered out)
      assertEquals(results.size(), 1);
      assertEquals(results.get(0).getData().getUrn(), testUrn1.toString());
    }
  }

  @org.testng.annotations.Test
  public void testBatchLoadWithEntityClientException() throws Exception {
    // Setup
    List<String> urns = Arrays.asList(testUrn1.toString());

    when(mockEntityClient.batchGetV2(
            eq(mockQueryContext.getOperationContext()),
            eq(Constants.TEST_ENTITY_NAME),
            any(HashSet.class),
            eq(TestType.ASPECTS_TO_FETCH)))
        .thenThrow(new RemoteInvocationException("Test exception"));

    // Execute & Verify
    try {
      testType.batchLoad(urns, mockQueryContext);
    } catch (Exception exception) {
      assertTrue(exception.getMessage().contains("Failed to batch load Tests"));
    }
  }

  @org.testng.annotations.Test
  public void testBatchLoadWithInvalidUrn() {
    // Setup
    List<String> urns = Arrays.asList("invalid-urn-format");

    // Execute & Verify
    try {
      testType.batchLoad(urns, mockQueryContext);
    } catch (Exception exception) {
      assertTrue(exception.getMessage().contains("Failed to convert urn string"));
    }
  }

  @org.testng.annotations.Test
  public void testBatchLoadEmptyList() throws Exception {
    // Setup
    List<String> urns = Arrays.asList();

    when(mockEntityClient.batchGetV2(
            eq(mockQueryContext.getOperationContext()),
            eq(Constants.TEST_ENTITY_NAME),
            any(HashSet.class),
            eq(TestType.ASPECTS_TO_FETCH)))
        .thenReturn(new HashMap<>());

    // Execute
    List<DataFetcherResult<Test>> results = testType.batchLoad(urns, mockQueryContext);

    // Verify
    assertTrue(results.isEmpty());
  }

  private EntityResponse createMockEntityResponse(Urn urn, boolean hasTestInfo) {
    EntityResponse entityResponse = mock(EntityResponse.class);
    when(entityResponse.getUrn()).thenReturn(urn);

    EnvelopedAspectMap aspectMap = mock(EnvelopedAspectMap.class);
    when(entityResponse.getAspects()).thenReturn(aspectMap);

    if (hasTestInfo) {
      EnvelopedAspect testInfoAspect = mock(EnvelopedAspect.class);
      when(aspectMap.get(Constants.TEST_INFO_ASPECT_NAME)).thenReturn(testInfoAspect);
    } else {
      when(aspectMap.get(Constants.TEST_INFO_ASPECT_NAME)).thenReturn(null);
    }

    return entityResponse;
  }

  private Test createMockTest(String urn) {
    Test test = mock(Test.class);
    when(test.getUrn()).thenReturn(urn);
    when(test.getType()).thenReturn(EntityType.TEST);
    return test;
  }
}
