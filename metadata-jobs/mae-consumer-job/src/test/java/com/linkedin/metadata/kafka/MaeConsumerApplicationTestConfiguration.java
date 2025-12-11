/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.auth.SystemAuthenticationFactory;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.models.registry.ConfigEntityRegistry;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchService;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.metadata.services.SecretService;
import io.ebean.Database;
import org.mockito.Answers;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@TestConfiguration
@Import(value = {SystemAuthenticationFactory.class})
public class MaeConsumerApplicationTestConfiguration {

  @MockBean private KafkaHealthChecker kafkaHealthChecker;

  @MockBean private EntityServiceImpl _entityServiceImpl;

  @MockBean private Database ebeanServer;

  @MockBean private EntityRegistry entityRegistry;

  @MockBean private RestrictedService restrictedService;

  @MockBean private SecretService secretService;

  @MockBean private GraphService _graphService;

  @MockBean private ElasticSearchSystemMetadataService _elasticSearchSystemMetadataService;

  @MockBean private ConfigEntityRegistry _configEntityRegistry;

  @MockBean public ElasticSearchService elasticSearchService;

  @MockBean public MetricUtils metricUtils;

  @MockBean(answer = Answers.RETURNS_MOCKS)
  public SearchClientShim<?> searchClientShim;
}
