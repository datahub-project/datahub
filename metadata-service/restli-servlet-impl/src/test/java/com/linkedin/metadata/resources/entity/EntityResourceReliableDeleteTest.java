package com.linkedin.metadata.resources.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

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
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.run.DeleteEntityResponse;
import com.linkedin.metadata.service.async.delete.DeleteEntityReport;
import com.linkedin.metadata.service.async.delete.ReliableHardDelete;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.parseq.Engine;
import com.linkedin.parseq.EngineBuilder;
import com.linkedin.parseq.Task;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** The flag only swaps the delete call; everything around it is today's code. */
public class EntityResourceReliableDeleteTest {
  private static final Urn URN =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,my_db.my_table,PROD)");

  private EntityResource resource;
  private EntityService<?> entityService;
  private ReliableHardDelete reliableHardDelete;
  private Engine engine;
  private ScheduledExecutorService timerScheduler;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    final TimeseriesAspectService timeseriesAspectService = mock(TimeseriesAspectService.class);
    reliableHardDelete = mock(ReliableHardDelete.class);
    final Authorizer authorizer = mock(Authorizer.class);
    when(authorizer.authorize(any(AuthorizationRequest.class)))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, ""));
    when(timeseriesAspectService.deleteAspectValues(
            any(OperationContext.class), anyString(), anyString(), any()))
        .thenReturn(new DeleteAspectValuesResult().setNumDocsDeleted(0L));
    when(entityService.deleteUrn(any(OperationContext.class), any(Urn.class)))
        .thenReturn(new RollbackRunResult(List.of(), 1, List.of()));

    resource = new EntityResource();
    resource.setEntityService(entityService);
    resource.setTimeseriesAspectService(timeseriesAspectService);
    resource.setAuthorizer(authorizer);
    resource.setSystemOperationContext(TestOperationContexts.systemContextNoSearchAuthorization());
    resource.setReliableHardDelete(reliableHardDelete);

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
  public void flagOnWholeEntityDeleteCallsTheReliableDeleteInsteadOfDeleteUrn() throws Exception {
    enable(true);
    when(reliableHardDelete.delete(any(), eq(URN)))
        .thenReturn(
            new DeleteEntityReport(
                URN.toString(),
                ConditionalDeleteOutcome.DELETED,
                12L,
                new RollbackRunResult(List.of(), 12, List.of())));

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, null, null));

    verify(reliableHardDelete).delete(any(), eq(URN));
    verify(entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
    assertEquals(response.getRows().longValue(), 12L);
  }

  @Test
  public void flagOffWholeEntityDeleteIsTodays() throws Exception {
    enable(false);

    final DeleteEntityResponse response =
        await(resource.deleteEntity(URN.toString(), null, null, null));

    verify(entityService).deleteUrn(any(OperationContext.class), eq(URN));
    verify(reliableHardDelete, never()).delete(any(), any());
    assertEquals(response.getRows().longValue(), 1L);
  }

  private void enable(final boolean enabled) {
    when(reliableHardDelete.isEnabled()).thenReturn(enabled);
  }

  private <T> T await(final Task<T> task) {
    engine.blockingRun(task);
    return task.get();
  }
}
