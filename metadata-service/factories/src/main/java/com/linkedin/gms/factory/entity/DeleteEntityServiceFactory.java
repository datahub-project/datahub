package com.linkedin.gms.factory.entity;

import com.linkedin.metadata.entity.DeleteEntityService;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.metadata.utils.objectstorage.ObjectStorageClient;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import({EntityServiceFactory.class})
public class DeleteEntityServiceFactory {
  @Autowired
  @Qualifier("entityService")
  private EntityService<?> _entityService;

  @Autowired
  @Qualifier("graphService")
  private GraphService _graphService;

  @Autowired
  @Qualifier("entitySearchService")
  private EntitySearchService _entitySearchService;

  @Autowired(required = false)
  @Qualifier("objectStorageClient")
  @Nullable
  private ObjectStorageClient _objectStorageClient;

  @Autowired(required = false)
  @Nullable
  private MetricUtils _metricUtils;

  @Bean(name = "deleteEntityService")
  @Nonnull
  protected DeleteEntityService createDeleteEntityService() {
    return new DeleteEntityService(
        _entityService, _graphService, _entitySearchService, _objectStorageClient, _metricUtils);
  }
}
