package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

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
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
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
}
