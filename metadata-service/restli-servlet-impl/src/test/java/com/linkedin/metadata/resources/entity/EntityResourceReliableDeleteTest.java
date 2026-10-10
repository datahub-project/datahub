package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.HardDeleteService;
import com.linkedin.metadata.entity.RollbackResult;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.models.EntitySpecUtils;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.run.DeleteReferencesResponse;
import com.linkedin.metadata.run.RelatedAspectArray;
import com.linkedin.metadata.service.HardDeleteDispatcher;
import com.linkedin.metadata.service.HardDeleteRequest;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The Rest.li deletes run through {@link HardDeleteService}: with nothing handed over the response
 * is today's; handed over, the same response type with zero counts.
 */
public class EntityResourceReliableDeleteTest {
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");
  private static final DeleteCeiling CEILING = new DeleteCeiling(Map.of("datasetKey", 1L), 1L);

  private final OperationContext systemOpContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityResource resource;
  private EntityService<?> entityService;
  private DeleteEntityService deleteEntityService;
  private TimeseriesAspectService timeseriesAspectService;
  private HardDeleteDispatcher dispatcher;
  private Engine engine;
  private ScheduledExecutorService timerScheduler;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    deleteEntityService = mock(DeleteEntityService.class);
    timeseriesAspectService = mock(TimeseriesAspectService.class);
    dispatcher = mock(HardDeleteDispatcher.class);
    final Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, ""));
    when(timeseriesAspectService.deleteAspectValues(
            any(OperationContext.class), anyString(), anyString(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(1L));
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.of(CEILING));
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(new RollbackRunResult(List.of(), 1, List.of()));
    when(entityService.deleteUrn(any(OperationContext.class), eq(URN), eq(CEILING)))
        .thenReturn(
            new RollbackRunResult(
                List.of(),
                12,
                List.of(
                    new RollbackResult(
                        URN,
                        "dataset",
                        "datasetKey",
                        null,
                        null,
                        null,
                        null,
                        ChangeType.DELETE,
                        true,
                        0))));
    when(deleteEntityService.deleteReferencesTo(any(), eq(URN), anyBoolean()))
        .thenReturn(
            new DeleteReferencesResponse().setTotal(3).setRelatedAspects(new RelatedAspectArray()));

    resource = new EntityResource();
    resource.setEntityService(entityService);
    resource.setTimeseriesAspectService(timeseriesAspectService);
    resource.setDeleteEntityService(deleteEntityService);
    resource.setAuthorizer(authorizer);
    resource.setSystemOperationContext(systemOpContext);

    final Authentication authentication = mock(Authentication.class);
    when(authentication.getActor()).thenReturn(new Actor(ActorType.USER, "user"));
    AuthenticationContext.setAuthentication(authentication);
    timerScheduler = Executors.newSingleThreadScheduledExecutor();
    engine =
        new EngineBuilder()
            .setTaskExecutor(Runnable::run)
            .setTimerScheduler(timerScheduler)
            .build();
  }

  @AfterMethod
  public void tearDown() {
    engine.shutdown();
    timerScheduler.shutdownNow();
    AuthenticationContext.remove();
  }

  @Test
  public void flagOnWholeEntityDeleteIsTheBoundedDeleteThenTheTimeseries() throws Exception {
    use(true, null);

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, 10L, 20L));

    verify(entityService).deleteUrn(any(), eq(URN), eq(CEILING));
    verify(entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
    assertEquals(response.getUrn(), URN.toString());
    assertEquals(response.getRows().longValue(), 12L);
    assertEquals(response.getTimeseriesRows().longValue(), (long) timeseriesAspects().size());
  }

  @Test
  public void flagOffWholeEntityDeleteIsTodays() throws Exception {
    use(false, null);

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, null, null));

    verify(entityService).deleteUrn(any(OperationContext.class), eq(URN));
    assertEquals(response.getRows().longValue(), 1L);
    assertEquals(response.getTimeseriesRows().longValue(), (long) timeseriesAspects().size());
  }

  /** Handed over: the request carries the aspects and the window; zero counts come back. */
  @Test
  public void wholeEntityDeleteTakenReturnsZeroCountsAndDeletesNothingHere() throws Exception {
    use(true, dispatcher);
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, 10L, 20L));

    verify(dispatcher)
        .dispatch(
            any(),
            eq(HardDeleteRequest.entityAndTimeseries(URN, CEILING, timeseriesAspects(), 10L, 20L)));
    verify(entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
    verify(entityService, never()).deleteUrn(any(), any(), any(DeleteCeiling.class));
    verifyNoInteractions(timeseriesAspectService);
    assertEquals(response.getUrn(), URN.toString());
    assertEquals(response.getRows().longValue(), 0L);
    assertEquals(response.getTimeseriesRows().longValue(), 0L);
  }

  /** Deleting one timeseries aspect deletes no entity, so nothing is offered. */
  @Test
  public void anAspectOnlyDeleteIsNeverOffered() throws Exception {
    use(true, dispatcher);
    final String aspect = timeseriesAspects().get(0);

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), aspect, null, null));

    verifyNoInteractions(dispatcher);
    verify(timeseriesAspectService).deleteAspectValues(any(), eq("dataset"), eq(aspect), any());
    assertEquals(response.getTimeseriesRows().longValue(), 1L);
  }

  @Test
  public void deleteReferencesWithoutDispatcherIsTodays() throws Exception {
    use(true, null);

    final DeleteReferencesResponse response =
        await(resource.deleteReferencesTo(URN.toString(), false));

    verify(deleteEntityService).deleteReferencesTo(any(), eq(URN), eq(false));
    assertEquals(response.getTotal().intValue(), 3);
  }

  @Test
  public void deleteReferencesTakenReturnsAnEmptyResponse() throws Exception {
    use(false, dispatcher);
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    final DeleteReferencesResponse response =
        await(resource.deleteReferencesTo(URN.toString(), false));

    verify(dispatcher).dispatch(any(), eq(HardDeleteRequest.references(URN)));
    verify(deleteEntityService, never()).deleteReferencesTo(any(), any(), anyBoolean());
    assertEquals(response.getTotal().intValue(), 0);
    assertTrue(response.getRelatedAspects().isEmpty());
  }

  /** A dry run is a read: it is never offered. */
  @Test
  public void aDryRunIsNeverOffered() throws Exception {
    use(true, dispatcher);

    final DeleteReferencesResponse response =
        await(resource.deleteReferencesTo(URN.toString(), true));

    verifyNoInteractions(dispatcher);
    verify(deleteEntityService).deleteReferencesTo(any(), eq(URN), eq(true));
    assertEquals(response.getTotal().intValue(), 3);
  }

  private void use(final boolean reliableHardDelete, final HardDeleteDispatcher hardDispatcher) {
    resource.setHardDeleteService(
        new HardDeleteService(
            entityService,
            deleteEntityService,
            timeseriesAspectService,
            new ReliableHardDelete(entityService, reliableHardDelete),
            hardDispatcher));
  }

  private List<String> timeseriesAspects() {
    return EntitySpecUtils.getEntityTimeseriesAspectNames(
        systemOpContext.getEntityRegistry(), URN.getEntityType());
  }

  private <T> T await(final Task<T> task) {
    engine.blockingRun(task);
    return task.get();
  }
}
