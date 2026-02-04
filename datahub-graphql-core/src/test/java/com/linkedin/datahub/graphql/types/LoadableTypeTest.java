package com.linkedin.datahub.graphql.types;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.authorization.AuthorizationUtils;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.Restricted;
import com.linkedin.entity.EntityResponse;
import graphql.execution.DataFetcherResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.mockito.MockedStatic;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class LoadableTypeTest {

  private QueryContext mockContext;
  private OperationContext mockOperationContext;
  private RestrictedService mockRestrictedService;

  // Test implementation of LoadableType using Restricted as the entity type to test
  // createRestrictedResult
  private static class TestLoadableType implements LoadableType<Restricted, String> {
    private final RestrictedService restrictedService;

    TestLoadableType(RestrictedService restrictedService) {
      this.restrictedService = restrictedService;
    }

    @Override
    public Class<Restricted> objectClass() {
      return Restricted.class;
    }

    @Override
    public RestrictedService getRestrictedService() {
      return restrictedService;
    }

    @Override
    public List<DataFetcherResult<Restricted>> batchLoadWithoutAuthorization(
        @Nonnull List<String> keys, @Nonnull QueryContext context) {
      return keys.stream()
          .map(
              key -> {
                Restricted entity = new Restricted();
                entity.setUrn(key);
                entity.setType(EntityType.RESTRICTED);
                return DataFetcherResult.<Restricted>newResult().data(entity).build();
              })
          .toList();
    }
  }

  @BeforeMethod
  public void setup() {
    mockContext = mock(QueryContext.class);
    mockOperationContext = mock(OperationContext.class);
    mockRestrictedService = mock(RestrictedService.class);
    when(mockContext.getOperationContext()).thenReturn(mockOperationContext);
  }

  @Test
  public void testGetKeyToUrnDefault() {
    TestLoadableType loadableType = new TestLoadableType(null);

    String urnStr = "urn:li:dataset:(urn:li:dataPlatform:hive,testDataset,PROD)";
    Urn result = loadableType.getKeyToUrn().apply(urnStr);

    assertNotNull(result);
    assertEquals(result.toString(), urnStr);
  }

  @Test
  public void testGetRestrictedServiceDefault() {
    // Test with null RestrictedService
    TestLoadableType loadableType = new TestLoadableType(null);
    assertNull(loadableType.getRestrictedService());

    // Test with provided RestrictedService
    TestLoadableType loadableTypeWithService = new TestLoadableType(mockRestrictedService);
    assertEquals(loadableTypeWithService.getRestrictedService(), mockRestrictedService);
  }

  @Test
  public void testCreateRestrictedResultWithService() {
    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Urn encryptedUrn = UrnUtils.getUrn("urn:li:restricted:v2:encrypted123");
    when(mockRestrictedService.encryptRestrictedUrn(testUrn)).thenReturn(encryptedUrn);

    TestLoadableType loadableType = new TestLoadableType(mockRestrictedService);

    DataFetcherResult<Restricted> result = loadableType.createRestrictedResult(testUrn);

    assertNotNull(result);
    assertNotNull(result.getData());
    Restricted restricted = result.getData();
    assertEquals(restricted.getType(), EntityType.RESTRICTED);
    assertEquals(restricted.getUrn(), encryptedUrn.toString());
  }

  @Test
  public void testCreateRestrictedResultWithoutService() {
    Urn testUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");

    TestLoadableType loadableType = new TestLoadableType(null);

    DataFetcherResult<Restricted> result = loadableType.createRestrictedResult(testUrn);

    assertNotNull(result);
    assertNotNull(result.getData());
    Restricted restricted = result.getData();
    assertEquals(restricted.getType(), EntityType.RESTRICTED);
    // Without service, original URN is used
    assertEquals(restricted.getUrn(), testUrn.toString());
  }

  @Test
  public void testMapResponsesToBatchResults() {
    TestLoadableType loadableType = new TestLoadableType(null);

    List<String> urnStrs =
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds3,PROD)");

    Map<Urn, EntityResponse> responseMap = new HashMap<>();
    Urn urn1 = UrnUtils.getUrn(urnStrs.get(0));
    Urn urn3 = UrnUtils.getUrn(urnStrs.get(2));

    EntityResponse response1 = mock(EntityResponse.class);
    EntityResponse response3 = mock(EntityResponse.class);
    responseMap.put(urn1, response1);
    responseMap.put(urn3, response3);
    // Note: urn2 is intentionally missing

    List<DataFetcherResult<Restricted>> results =
        loadableType.mapResponsesToBatchResults(
            urnStrs,
            responseMap,
            (ctx, resp) -> {
              Restricted entity = new Restricted();
              entity.setUrn(ctx.toString());
              entity.setType(EntityType.RESTRICTED);
              return entity;
            },
            mockContext);

    assertEquals(results.size(), 3);
    assertNotNull(results.get(0)); // urn1 exists
    assertNull(results.get(1)); // urn2 is missing
    assertNotNull(results.get(2)); // urn3 exists
  }

  @Test
  public void testMapResponsesToBatchResultsEmptyList() {
    TestLoadableType loadableType = new TestLoadableType(null);

    List<DataFetcherResult<Restricted>> results =
        loadableType.mapResponsesToBatchResults(
            Arrays.asList(), new HashMap<>(), (ctx, resp) -> new Restricted(), mockContext);

    assertEquals(results.size(), 0);
  }

  @Test
  public void testBatchLoadWithAuthorization() throws Exception {
    Urn encryptedUrn = UrnUtils.getUrn("urn:li:restricted:v2:encrypted123");
    when(mockRestrictedService.encryptRestrictedUrn(any())).thenReturn(encryptedUrn);

    TestLoadableType loadableType = new TestLoadableType(mockRestrictedService);

    List<String> keys =
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:hive,authorized,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,unauthorized,PROD)");

    try (MockedStatic<AuthorizationUtils> authUtils = mockStatic(AuthorizationUtils.class)) {
      // First key is authorized, second is not
      authUtils
          .when(() -> AuthorizationUtils.canView(any(), any(Urn.class)))
          .thenAnswer(
              invocation -> {
                Urn urn = invocation.getArgument(1);
                return urn.toString().contains("authorized")
                    && !urn.toString().contains("unauthorized");
              });

      List<DataFetcherResult<Restricted>> results = loadableType.batchLoad(keys, mockContext);

      assertEquals(results.size(), 2);

      // First result should be the actual entity (authorized)
      assertNotNull(results.get(0));
      assertNotNull(results.get(0).getData());
      assertEquals(results.get(0).getData().getUrn(), keys.get(0));

      // Second result should be a Restricted entity with encrypted URN (unauthorized)
      assertNotNull(results.get(1));
      assertNotNull(results.get(1).getData());
      assertEquals(results.get(1).getData().getType(), EntityType.RESTRICTED);
      assertEquals(results.get(1).getData().getUrn(), encryptedUrn.toString());
    }
  }

  @Test
  public void testBatchLoadAllAuthorized() throws Exception {
    TestLoadableType loadableType = new TestLoadableType(null);

    List<String> keys =
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds2,PROD)");

    try (MockedStatic<AuthorizationUtils> authUtils = mockStatic(AuthorizationUtils.class)) {
      // All keys are authorized
      authUtils.when(() -> AuthorizationUtils.canView(any(), any(Urn.class))).thenReturn(true);

      List<DataFetcherResult<Restricted>> results = loadableType.batchLoad(keys, mockContext);

      assertEquals(results.size(), 2);

      for (int i = 0; i < keys.size(); i++) {
        assertNotNull(results.get(i));
        assertNotNull(results.get(i).getData());
        assertEquals(results.get(i).getData().getUrn(), keys.get(i));
      }
    }
  }

  @Test
  public void testBatchLoadNoneAuthorized() throws Exception {
    Urn encryptedUrn = UrnUtils.getUrn("urn:li:restricted:v2:encrypted123");
    when(mockRestrictedService.encryptRestrictedUrn(any())).thenReturn(encryptedUrn);

    TestLoadableType loadableType = new TestLoadableType(mockRestrictedService);

    List<String> keys =
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:hive,ds2,PROD)");

    try (MockedStatic<AuthorizationUtils> authUtils = mockStatic(AuthorizationUtils.class)) {
      // No keys are authorized
      authUtils.when(() -> AuthorizationUtils.canView(any(), any(Urn.class))).thenReturn(false);

      List<DataFetcherResult<Restricted>> results = loadableType.batchLoad(keys, mockContext);

      assertEquals(results.size(), 2);

      // All results should be Restricted entities with encrypted URNs
      for (DataFetcherResult<Restricted> result : results) {
        assertNotNull(result);
        assertNotNull(result.getData());
        assertEquals(result.getData().getType(), EntityType.RESTRICTED);
        assertEquals(result.getData().getUrn(), encryptedUrn.toString());
      }
    }
  }

  @Test
  public void testLoad() throws Exception {
    TestLoadableType loadableType = new TestLoadableType(null);
    String key = "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";

    try (MockedStatic<AuthorizationUtils> authUtils = mockStatic(AuthorizationUtils.class)) {
      authUtils.when(() -> AuthorizationUtils.canView(any(), any(Urn.class))).thenReturn(true);

      DataFetcherResult<Restricted> result = loadableType.load(key, mockContext);

      assertNotNull(result);
      assertNotNull(result.getData());
      assertEquals(result.getData().getUrn(), key);
    }
  }

  @Test
  public void testName() {
    TestLoadableType loadableType = new TestLoadableType(null);

    assertEquals(loadableType.name(), "Restricted");
  }
}
