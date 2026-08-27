package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.FabricType;
import com.linkedin.data.template.StringArray;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.FilterExistingUrnsRequest;
import com.linkedin.metadata.authorization.EntityAuthorizationUtils;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Entity;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.restli.internal.server.methods.AnyRecord;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityResourceTest {
  private EntityResource entityResource;
  private EntityService<?> entityService;
  private TimeseriesAspectService timeseriesAspectService;
  private Authorizer authorizer;
  private OperationContext systemOperationContext;
  private Engine parseqEngine;

  @BeforeMethod
  public void setup() {
    entityResource = new EntityResource();
    entityService = mock(EntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation -> {
              AuthorizationRequest request = invocation.getArgument(0);
              return new AuthorizationResult(request, AuthorizationResult.Type.ALLOW, "allowed");
            });
    when(timeseriesAspectService.deleteAspectValues(
            any(OperationContext.class), any(), any(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(0L));
    systemOperationContext = TestOperationContexts.systemContextNoSearchAuthorization();

    entityResource.setEntityService(entityService);
    entityResource.setTimeseriesAspectService(timeseriesAspectService);
    entityResource.setAuthorizer(authorizer);
    entityResource.setSystemOperationContext(systemOperationContext);

    Authentication mockAuthentication = mock(Authentication.class);
    AuthenticationContext.setAuthentication(mockAuthentication);
    Actor actor = new Actor(ActorType.USER, "user");
    when(mockAuthentication.getActor()).thenReturn(actor);

    parseqEngine =
        new EngineBuilder()
            .setTaskExecutor(Runnable::run)
            .setTimerScheduler(Executors.newSingleThreadScheduledExecutor())
            .build();
  }

  @AfterMethod
  public void tearDown() {
    if (parseqEngine != null) {
      parseqEngine.shutdown();
    }
  }

  private <T> T awaitTask(Task<T> task) {
    parseqEngine.blockingRun(task);
    return task.get();
  }

  /**
   * Regression for INC-3052: when {@link RollbackRunResult#getRowsDeletedFromEntityDeletion()}
   * returns null (e.g. {@code RollbackResult.additionalRowsAffected} was never set), the previous
   * code unboxed null into the generated primitive-long {@code DeleteEntityResponse.setRows(long)}
   * and threw NullPointerException. Customer-driven mass CLI deletes hit this path repeatedly,
   * triggering High5xxRate. The fix defaults null to 0L.
   */
  @Test
  public void testDeleteEntityHandlesNullRowsDeletedFromEntityDeletion()
      throws URISyntaxException {
    RollbackRunResult resultWithNullRows =
        new RollbackRunResult(Collections.emptyList(), null, Collections.emptyList());
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(resultWithNullRows);

    Urn urn = new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD);

    Task<DeleteEntityResponse> task = entityResource.deleteEntity(urn.toString(), null, null, null);

    assertNotNull(task);
  }

  /**
   * Entities such as {@code structuredProperty} have no registered timeseries aspects. The delete
   * flow must not call {@link TimeseriesAspectService#deleteAspectValues} (or require timeseries
   * authorization) in that case, so entity delete succeeds when the user can delete the entity.
   */
  @Test
  public void testDeleteEntitySkipsTimeseriesWhenEntityHasNoTimeseriesAspects()
      throws URISyntaxException {
    RollbackRunResult rollback =
        new RollbackRunResult(Collections.emptyList(), 1, Collections.emptyList());
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(rollback);

    Urn urn = Urn.createFromString("urn:li:structuredProperty:business_definition");

    Task<DeleteEntityResponse> task = entityResource.deleteEntity(urn.toString(), null, null, null);

    assertNotNull(task);
    verify(timeseriesAspectService, never())
        .deleteAspectValues(any(OperationContext.class), any(), any(), any());
  }

  @Test
  public void testFilterExistingUrnsEmptyRequest() throws Exception {
    FilterExistingUrnsRequest request = new FilterExistingUrnsRequest();
    request.setUrns(new StringArray());

    String[] result = awaitTask(entityResource.filterExistingUrns(request));

    assertEquals(result.length, 0);
    verify(entityService, never())
        .exists(any(OperationContext.class), any(java.util.Collection.class), anyBoolean());
  }

  @Test
  public void testFilterExistingUrnsReturnsExistingSubset() throws Exception {
    Urn existingUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test,PROD)");
    Urn missingUrn = Urn.createFromString("urn:li:structuredProperty:deleted");

    when(entityService.exists(any(OperationContext.class), eq(Set.of(existingUrn, missingUrn)), eq(true)))
        .thenReturn(Set.of(existingUrn));

    FilterExistingUrnsRequest request = new FilterExistingUrnsRequest();
    request.setUrns(new StringArray(List.of(existingUrn.toString(), missingUrn.toString())));

    String[] result = awaitTask(entityResource.filterExistingUrns(request));

    assertEquals(result.length, 1);
    assertEquals(result[0], existingUrn.toString());
    verify(entityService, times(1))
        .exists(any(OperationContext.class), eq(Set.of(existingUrn, missingUrn)), eq(true));
  }

  @Test
  public void testFilterExistingUrnsExcludeSoftDeleted() throws Exception {
    Urn existingUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hdfs,test2,PROD)");

    when(entityService.exists(any(OperationContext.class), eq(Set.of(existingUrn)), eq(false)))
        .thenReturn(Set.of(existingUrn));

    FilterExistingUrnsRequest request = new FilterExistingUrnsRequest();
    request.setUrns(new StringArray(List.of(existingUrn.toString())));
    request.setIncludeSoftDelete(false);

    String[] result = awaitTask(entityResource.filterExistingUrns(request));

    assertEquals(result.length, 1);
    verify(entityService, times(1))
        .exists(any(OperationContext.class), eq(Set.of(existingUrn)), eq(false));
  }

  @Test
  public void testGetEntityUsesMetadataReadUsageOperation() throws Exception {
    Urn urn = new DatasetUrn(new DataPlatformUrn("platform"), "name", FabricType.PROD);
    Entity entity = new Entity(new DataMap());
    when(entityService.getEntity(any(OperationContext.class), eq(urn), any(), eq(true)))
        .thenReturn(entity);

    AnyRecord result = awaitTask(entityResource.get(urn.toString(), null));

    assertNotNull(result);
    verify(entityService, times(1)).getEntity(any(OperationContext.class), eq(urn), any(), eq(true));
  }

  /**
   * Regression coverage for the confirmed disclosure path where this deprecated, Snapshot-based
   * {@code get}/{@code batchGet} had no per-field mapper and no whole-aspect redaction call the
   * way v1/v2/v3 OpenAPI and Rest.li's {@code EntityV2Resource} gained, so a restricted aspect
   * (e.g. {@code viewProperties}) was fetched and returned unconditionally. The fix prunes the
   * restricted aspect name from the requested set before the fetch rather than redacting the
   * returned Snapshot afterward, so this asserts against what {@code entityService.getEntity} was
   * actually asked to fetch.
   */
  @Test
  public void testGetPrunesQuerySqlRestrictedAspectFromProjectedSet() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Entity entity = new Entity(new DataMap());
    when(entityService.getEntity(any(OperationContext.class), eq(urn), any(), eq(true)))
        .thenReturn(entity);

    try (MockedStatic<EntityAuthorizationUtils> mockedUtils =
        Mockito.mockStatic(EntityAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockedUtils
          .when(
              () ->
                  EntityAuthorizationUtils.isQuerySqlAspectRestricted(
                      any(OperationContext.class), eq(urn), eq("viewProperties")))
          .thenReturn(true);

      awaitTask(
          entityResource.get(
              urn.toString(), new String[] {"viewProperties", "datasetProperties"}));

      ArgumentCaptor<Set<String>> projectedAspectsCaptor = ArgumentCaptor.forClass(Set.class);
      verify(entityService)
          .getEntity(any(OperationContext.class), eq(urn), projectedAspectsCaptor.capture(), eq(true));
      assertFalse(
          projectedAspectsCaptor.getValue().contains("viewProperties"),
          "viewProperties must be pruned before the fetch when restricted");
      assertTrue(
          projectedAspectsCaptor.getValue().contains("datasetProperties"),
          "unrelated requested aspects must still be fetched");
    }
  }

  /**
   * Regression for a residual disclosure a follow-up audit caught in the fix above: when the
   * caller requests ONLY a restricted aspect, pruning it leaves an EMPTY set. {@code
   * EntityServiceImpl#getLatestAspect} treats an empty aspect-name set as "fetch every registered
   * aspect" — so passing that empty set through would silently widen the fetch back to
   * everything, restoring the very aspect just restricted (and leaking every other aspect too).
   * The fix must fall back to a non-empty, harmless set (the entity's own key aspect) instead of
   * ever letting the projected set go empty when something was actually requested.
   */
  @Test
  public void testGetFallsBackToKeyAspectWhenAllRequestedAspectsAreRestricted() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)");
    Entity entity = new Entity(new DataMap());
    when(entityService.getEntity(any(OperationContext.class), eq(urn), any(), eq(true)))
        .thenReturn(entity);

    try (MockedStatic<EntityAuthorizationUtils> mockedUtils =
        Mockito.mockStatic(EntityAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockedUtils
          .when(
              () ->
                  EntityAuthorizationUtils.isQuerySqlAspectRestricted(
                      any(OperationContext.class), eq(urn), eq("viewProperties")))
          .thenReturn(true);

      awaitTask(entityResource.get(urn.toString(), new String[] {"viewProperties"}));

      ArgumentCaptor<Set<String>> projectedAspectsCaptor = ArgumentCaptor.forClass(Set.class);
      verify(entityService)
          .getEntity(
              any(OperationContext.class), eq(urn), projectedAspectsCaptor.capture(), eq(true));
      assertFalse(
          projectedAspectsCaptor.getValue().isEmpty(),
          "an empty set here would be misread by the entity service as \"fetch everything\"");
      assertFalse(projectedAspectsCaptor.getValue().contains("viewProperties"));
    }
  }

  /**
   * Same fix, batch path: urns landing on different projected-aspect sets (here, only one of the
   * two has {@code viewProperties} restricted) must not share a single {@code getEntities} call
   * with the unpruned set, which would either over-withhold for the unrestricted urn or leak for
   * the restricted one depending on which way the shared set went.
   */
  @Test
  public void testBatchGetGroupsUrnsByDistinctProjectedAspectSets() throws Exception {
    Urn restrictedUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,restricted,PROD)");
    Urn allowedUrn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,allowed,PROD)");
    when(entityService.getEntities(any(OperationContext.class), any(), any(), eq(true)))
        .thenAnswer(
            invocation -> {
              Set<Urn> urns = invocation.getArgument(1);
              java.util.Map<Urn, Entity> out = new java.util.HashMap<>();
              urns.forEach(u -> out.put(u, new Entity(new DataMap())));
              return out;
            });

    try (MockedStatic<EntityAuthorizationUtils> mockedUtils =
        Mockito.mockStatic(EntityAuthorizationUtils.class, Mockito.CALLS_REAL_METHODS)) {
      mockedUtils
          .when(
              () ->
                  EntityAuthorizationUtils.isQuerySqlAspectRestricted(
                      any(OperationContext.class), eq(restrictedUrn), eq("viewProperties")))
          .thenReturn(true);
      mockedUtils
          .when(
              () ->
                  EntityAuthorizationUtils.isQuerySqlAspectRestricted(
                      any(OperationContext.class), eq(allowedUrn), eq("viewProperties")))
          .thenReturn(false);

      java.util.Map<String, AnyRecord> result =
          awaitTask(
              entityResource.batchGet(
                  Set.of(restrictedUrn.toString(), allowedUrn.toString()),
                  new String[] {"viewProperties", "datasetProperties"}));

      assertEquals(result.size(), 2);
      ArgumentCaptor<Set<String>> aspectsCaptor = ArgumentCaptor.forClass(Set.class);
      verify(entityService, times(2))
          .getEntities(any(OperationContext.class), any(), aspectsCaptor.capture(), eq(true));
      List<Set<String>> capturedAspectSets = aspectsCaptor.getAllValues();
      assertTrue(
          capturedAspectSets.stream().anyMatch(s -> !s.contains("viewProperties")),
          "the restricted urn's group must fetch without viewProperties");
      assertTrue(
          capturedAspectSets.stream().anyMatch(s -> s.contains("viewProperties")),
          "the allowed urn's group must still fetch viewProperties");
    }
  }

  @Test
  public void testSearchUsesSearchQueryUsageOperation() throws Exception {
    EntitySearchService entitySearchService = mock(EntitySearchService.class);
    when(entitySearchService.search(
            any(OperationContext.class),
            eq(List.of("dataset")),
            anyString(),
            any(),
            any(),
            eq(0),
            eq(10)))
        .thenReturn(
            new SearchResult()
                .setNumEntities(0)
                .setEntities(new SearchEntityArray())
                .setFrom(0)
                .setPageSize(10)
                .setMetadata(new SearchResultMetadata()));

    setEntitySearchService(entityResource, entitySearchService);

    SearchResult result =
        awaitTask(
            entityResource.search(
                "dataset", "*", null, null, null, 0, 10, null, new SearchFlags()));

    assertNotNull(result);
    verify(entitySearchService, times(1))
        .search(
            any(OperationContext.class),
            eq(List.of("dataset")),
            anyString(),
            any(),
            any(),
            eq(0),
            eq(10));
  }

  private static void setEntitySearchService(
      EntityResource entityResource, EntitySearchService entitySearchService) throws Exception {
    Field field = EntityResource.class.getDeclaredField("entitySearchService");
    field.setAccessible(true);
    field.set(entityResource, entitySearchService);
  }
}
