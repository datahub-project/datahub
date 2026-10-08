package com.linkedin.gms.factory.entity;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.DeleteCeiling;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.HardDeleteService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.service.HardDeleteDispatcher;
import com.linkedin.metadata.service.HardDeleteRequest;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * {@link ReliableHardDeleteFactory}'s hard-delete service: where hard deletes run is decided only
 * by whether a dispatcher bean exists.
 */
public class ReliableHardDeleteFactoryHardDeleteServiceTest {
  private static final Urn URN = UrnUtils.getUrn("urn:li:tag:wired");
  private static final DeleteCeiling CEILING = new DeleteCeiling(Map.of("tagKey", 1L), 1L);

  private final OperationContext opContext =
      TestOperationContexts.systemContextNoSearchAuthorization();
  private EntityService<?> entityService;

  @BeforeMethod
  @SuppressWarnings("unchecked")
  public void setup() {
    entityService = mock(EntityService.class);
    when(entityService.captureDeleteCeiling(any(), eq(URN))).thenReturn(Optional.of(CEILING));
    when(entityService.deleteUrn(any(), eq(URN)))
        .thenReturn(new RollbackRunResult(List.of(), 1, List.of()));
  }

  @Test
  public void withoutADispatcherBeanTheDeleteRunsInProcess() {
    try (AnnotationConfigApplicationContext context = context(null)) {
      context.getBean(HardDeleteService.class).deleteEntity(opContext, URN);
    }

    verify(entityService).deleteUrn(any(), eq(URN));
  }

  /** The dispatcher is used whatever the reliableHardDelete flag says (off here). */
  @Test
  public void aDispatcherBeanIsOfferedTheDelete() {
    final HardDeleteDispatcher dispatcher = mock(HardDeleteDispatcher.class);
    when(dispatcher.dispatch(any(), any())).thenReturn(true);

    try (AnnotationConfigApplicationContext context = context(dispatcher)) {
      context.getBean(HardDeleteService.class).deleteEntity(opContext, URN);
    }

    verify(dispatcher).dispatch(opContext, HardDeleteRequest.entity(URN, CEILING));
    verify(entityService, never()).deleteUrn(any(), any());
  }

  private AnnotationConfigApplicationContext context(final HardDeleteDispatcher dispatcher) {
    final FeatureFlags featureFlags = new FeatureFlags();
    featureFlags.setReliableHardDelete(false);
    final ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getFeatureFlags()).thenReturn(featureFlags);

    final AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
    context.registerBean("entityService", EntityService.class, () -> entityService);
    context.registerBean(
        "deleteEntityService", DeleteEntityService.class, () -> mock(DeleteEntityService.class));
    context.registerBean(
        "timeseriesAspectService",
        TimeseriesAspectService.class,
        () -> mock(TimeseriesAspectService.class));
    context.registerBean(
        "configurationProvider", ConfigurationProvider.class, () -> configurationProvider);
    if (dispatcher != null) {
      context.registerBean(HardDeleteDispatcher.class, () -> dispatcher);
    }
    context.register(ReliableHardDeleteFactory.class);
    context.refresh();
    return context;
  }
}
