package com.linkedin.metadata.entity.retention;

import static org.mockito.Mockito.mock;

import com.linkedin.entity.client.EntityClientConfig;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.client.SystemJavaEntityClient;
import com.linkedin.metadata.config.cache.client.EntityClientCacheConfig;
import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.event.EventProducer;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.LineageSearchService;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.metadata.search.client.CachingEntitySearchService;
import com.linkedin.metadata.service.RollbackService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;

/** Builds {@link SystemEntityClient} instances for retention-service unit and integration tests. */
public final class RetentionTestUtils {

  private RetentionTestUtils() {}

  /**
   * A {@link SystemEntityClient} backed by the given {@link EntityService}, with the entity/aspect
   * cache disabled so reads always reflect the latest state written by the test through the same
   * (or another) {@link EntityService} instance. The search/timeseries/rollback dependencies are
   * mocked since retention resolution and invalidation never touch them.
   */
  @Nonnull
  public static SystemEntityClient systemEntityClient(
      @Nonnull EntityService<?> entityService,
      @Nonnull EventProducer eventProducer,
      @Nonnull MetricUtils metricUtils) {
    return new SystemJavaEntityClient(
        entityService,
        mock(DeleteEntityService.class),
        mock(EntitySearchService.class),
        mock(CachingEntitySearchService.class),
        mock(SearchService.class),
        mock(LineageSearchService.class),
        mock(TimeseriesAspectService.class),
        mock(RollbackService.class),
        eventProducer,
        new EntityClientCacheConfig(),
        EntityClientConfig.builder().batchGetV2Size(1).build(),
        metricUtils);
  }
}
