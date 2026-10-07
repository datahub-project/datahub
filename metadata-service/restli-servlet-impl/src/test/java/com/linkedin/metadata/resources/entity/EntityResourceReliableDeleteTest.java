package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.expectThrows;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.entity.ConditionalDeleteOutcome;
import com.linkedin.metadata.entity.DeleteCascadeListener;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.models.EntitySpecUtils;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RelatedAspectArray;
import com.linkedin.metadata.service.async.delete.DeleteEntityReport;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.concurrent.Executors;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class EntityResourceReliableDeleteTest {
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

  private EntityResource resource;
  private EntityService<?> entityService;
  private TimeseriesAspectService timeseriesAspectService;
  private DeleteEntityService deleteEntityService;
  private ReliableHardDelete reliableHardDelete;
  private Authorizer authorizer;
  private Engine engine;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    deleteEntityService = mock(DeleteEntityService.class);
    reliableHardDelete = mock(ReliableHardDelete.class);
    authorizer = mock(Authorizer.class);
    allow(true);
    when(timeseriesAspectService.deleteAspectValues(
            any(OperationContext.class), any(), any(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(0L));
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(new RollbackRunResult(List.of(), 1, List.of()));

    resource = new EntityResource();
    resource.setEntityService(entityService);
    resource.setTimeseriesAspectService(timeseriesAspectService);
    resource.setAuthorizer(authorizer);
    resource.setSystemOperationContext(TestOperationContexts.systemContextNoSearchAuthorization());
    resource.setDeleteEntityService(deleteEntityService);
    resource.setReliableHardDelete(reliableHardDelete);

    final Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "user"));
    AuthenticationContext.setAuthentication(authentication);
    engine =
        new EngineBuilder()
            .setTaskExecutor(Runnable::run)
            .setTimerScheduler(Executors.newSingleThreadScheduledExecutor())
            .build();
  }

  @AfterMethod
  public void tearDown() {
    engine.shutdown();
  }

  @Test
  public void enabledWholeEntityDeleteUsesTheReliableDeleteAndKeepsTheResponseShape()
      throws Exception {
    enable(true);
    when(reliableHardDelete.delete(any(), eq(URN)))
        .thenReturn(
            new DeleteEntityReport(URN.toString(), ConditionalDeleteOutcome.DELETED, 12L, 3L, 2));

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, null, null));

    assertEquals(response.getUrn(), URN.toString());
    assertEquals(response.getRows().longValue(), 12L);
    assertEquals(response.getTimeseriesRows().longValue(), 3L);
    verify(entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
    verifyNoInteractions(timeseriesAspectService);
  }

  @Test
  public void enabledTimeseriesAspectDeleteKeepsTodaysPath() throws Exception {
    enable(true);

    assertNotNull(await(resource.deleteEntity(URN.toString(), "datasetProfile", null, null)));

    verify(reliableHardDelete, never()).delete(any(), any());
    verify(timeseriesAspectService)
        .deleteAspectValues(any(), eq("dataset"), eq("datasetProfile"), any());
  }

  @Test
  public void enabledDeleteWithATimeWindowKeepsTodaysPath() throws Exception {
    enable(true);

    await(resource.deleteEntity(URN.toString(), null, 1L, null));

    verify(reliableHardDelete, never()).delete(any(), any());
    verify(entityService).deleteUrn(any(OperationContext.class), eq(URN));
  }

  /** Flag off: today's delete, today's unbounded timeseries delete and today's response. */
  @Test
  public void disabledDeleteIsTodays() throws Exception {
    enable(false);
    when(timeseriesAspectService.deleteAspectValues(
            any(OperationContext.class), eq("dataset"), anyString(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(2L));

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, null, null));

    verify(entityService).deleteUrn(any(OperationContext.class), eq(URN));
    verify(reliableHardDelete, never()).delete(any(), any());
    final int timeseriesAspects =
        EntitySpecUtils.getEntityTimeseriesAspectNames(
                TestOperationContexts.systemContextNoSearchAuthorization().getEntityRegistry(),
                "dataset")
            .size();
    verify(timeseriesAspectService, times(timeseriesAspects))
        .deleteAspectValues(any(OperationContext.class), eq("dataset"), anyString(), any());
    assertEquals(response.getUrn(), URN.toString());
    assertEquals(response.getRows().longValue(), 1L);
    assertEquals(response.getTimeseriesRows().longValue(), 2L * timeseriesAspects);
  }

  @Test
  public void aFailedReliableDeleteIsAServerError() {
    enable(true);
    when(reliableHardDelete.delete(any(), eq(URN)))
        .thenThrow(new IllegalStateException("did not finish within 55 seconds"));

    final RuntimeException thrown =
        expectThrows(
            RuntimeException.class, () -> resource.deleteEntity(URN.toString(), null, null, null));

    assertEquals(restLiCause(thrown).getStatus().getCode(), 500);
  }

  @Test
  public void unauthorizedDeleteStartsNothing() {
    enable(true);
    allow(false);

    final RuntimeException thrown =
        expectThrows(
            RuntimeException.class, () -> resource.deleteEntity(URN.toString(), null, null, null));

    assertEquals(restLiCause(thrown).getStatus().getCode(), 403);
    verify(reliableHardDelete, never()).delete(any(), any());
  }

  @Test
  public void enabledDeleteReferencesRemovesThemReliablyAndKeepsTheResponseShape()
      throws Exception {
    enable(true);
    when(deleteEntityService.removeReferencesResumable(
            any(), eq(URN), isNull(), eq(DeleteCascadeListener.NOOP)))
        .thenReturn(5);

    final DeleteReferencesResponse response =
        await(resource.deleteReferencesTo(URN.toString(), false));

    assertEquals(response.getTotal().intValue(), 5);
    assertEquals(response.getRelatedAspects().size(), 0);
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), eq(false));
  }

  @Test
  public void enabledDeleteReferencesFailsWhenAReferrerCouldNotBeCleaned() {
    enable(true);
    when(deleteEntityService.removeReferencesResumable(any(), eq(URN), isNull(), any()))
        .thenThrow(new IllegalStateException("Write of domains to urn:li:dataset:x was not committed"));

    final RuntimeException thrown =
        expectThrows(
            RuntimeException.class, () -> resource.deleteReferencesTo(URN.toString(), false));

    assertEquals(restLiCause(thrown).getStatus().getCode(), 500);
  }

  @Test
  public void enabledDryRunIsTodays() throws Exception {
    enable(true);
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(true)))
        .thenReturn(
            new DeleteReferencesResponse().setTotal(2).setRelatedAspects(new RelatedAspectArray()));

    assertEquals(
        await(resource.deleteReferencesTo(URN.toString(), true)).getTotal().intValue(), 2);
    verify(deleteEntityService, never()).removeReferencesResumable(any(), any(), any(), any());
  }

  @Test
  public void disabledDeleteReferencesIsTodays() throws Exception {
    enable(false);
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), eq(false)))
        .thenReturn(
            new DeleteReferencesResponse().setTotal(3).setRelatedAspects(new RelatedAspectArray()));

    assertEquals(
        await(resource.deleteReferencesTo(URN.toString(), false)).getTotal().intValue(), 3);
    verify(deleteEntityService, never()).removeReferencesResumable(any(), any(), any(), any());
  }

  private void enable(final boolean enabled) {
    when(reliableHardDelete.isEnabled()).thenReturn(enabled);
  }

  private void allow(final boolean allowed) {
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0),
                    allowed ? AuthorizationResult.Type.ALLOW : AuthorizationResult.Type.DENY,
                    ""));
  }

  private <T> T await(final Task<T> task) {
    engine.blockingRun(task);
    return task.get();
  }

  private static RestLiServiceException restLiCause(final Throwable thrown) {
    Throwable current = thrown;
    while (current != null && !(current instanceof RestLiServiceException)) {
      current = current.getCause();
    }
    assertNotNull(current, "no RestLiServiceException in the cause chain of " + thrown);
    return (RestLiServiceException) current;
  }
}
