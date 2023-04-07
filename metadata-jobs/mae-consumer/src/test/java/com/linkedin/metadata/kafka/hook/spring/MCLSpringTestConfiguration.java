package com.linkedin.metadata.kafka.hook.spring;

import com.datahub.authentication.Authentication;
import com.datahub.metadata.ingestion.IngestionScheduler;
import com.linkedin.entity.client.RestliEntityClient;
import com.linkedin.metadata.boot.kafka.DataHubUpgradeKafkaListener;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.transformer.SearchDocumentTransformer;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


@Configuration
@ComponentScan(basePackages = {
    "com.linkedin.metadata.kafka"
})
public class MCLSpringTestConfiguration {

  @MockBean
  public EntityRegistry entityRegistry;

  @MockBean
  public GraphService graphService;

  @MockBean
  public TimeseriesAspectService timeseriesAspectService;

  @MockBean
  public SystemMetadataService systemMetadataService;

  @MockBean
  public SearchDocumentTransformer searchDocumentTransformer;

  @MockBean
  public IngestionScheduler ingestionScheduler;

  @MockBean
  public RestliEntityClient entityClient;

  @MockBean
  public EntitySearchService searchService;

  @MockBean
  public Authentication systemAuthentication;

  @MockBean(name = "dataHubUpgradeKafkaListener")
  public DataHubUpgradeKafkaListener dataHubUpgradeKafkaListener;
}
