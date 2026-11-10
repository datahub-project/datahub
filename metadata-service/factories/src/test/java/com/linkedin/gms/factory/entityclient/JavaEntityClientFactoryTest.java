package com.linkedin.gms.factory.entityclient;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.HardDeleteService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import org.springframework.beans.factory.ObjectProvider;
import org.testng.annotations.Test;

/**
 * The {@code systemEntityClient} bean hard-deletes through the {@code hardDeleteService} bean.
 * Before, it was built without one, so every service on the system client (data products and
 * others) hard-deleted in-process instead of going through it.
 */
public class JavaEntityClientFactoryTest {

  private static final Urn URN = UrnUtils.getUrn("urn:li:dataProduct:systemClient");

  @Test
  @SuppressWarnings("unchecked")
  public void theSystemClientDeletesThroughTheHardDeleteServiceBean() throws Exception {
    final EntityService<?> entityService = mock(EntityService.class);
    final HardDeleteService hardDeleteService = mock(HardDeleteService.class);
    final ObjectProvider<HardDeleteService> provider = mock(ObjectProvider.class);
    when(provider.getIfAvailable()).thenReturn(hardDeleteService);
    final OperationContext opContext = mock(OperationContext.class);

    final SystemEntityClient client =
        new JavaEntityClientFactory()
            .systemEntityClient(
                entityService,
                mock(DeleteEntityService.class),
                mock(SearchService.class),
                mock(EntitySearchService.class),
                mock(CachingEntitySearchService.class),
                mock(TimeseriesAspectService.class),
                mock(LineageSearchService.class),
                mock(EventProducer.class),
                mock(RollbackService.class),
                new EntityClientCacheConfig(),
                EntityClientConfig.builder().batchGetV2Size(1).build(),
                mock(MetricUtils.class),
                provider);

    client.deleteEntity(opContext, URN);

    verify(hardDeleteService).deleteEntity(opContext, URN);
    verify(entityService, never()).deleteUrn(any(OperationContext.class), any(Urn.class));
  }
}
