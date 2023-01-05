package com.linkedin.datahub.upgrade.buildindices;

import com.google.common.collect.ImmutableList;
import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.shared.ElasticSearchIndexed;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.TopicConvention;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;


public class BuildIndices implements Upgrade {

    private final List<UpgradeStep> _steps;

    public BuildIndices(final SystemMetadataService systemMetadataService, final TimeseriesAspectService timeseriesAspectService,
                        final EntitySearchService entitySearchService, final GraphService graphService,
                        final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
                        final Producer<String, ? extends IndexedRecord> producer,
                        final TopicConvention convention, final GitVersion gitVersion, final KafkaHealthChecker kafkaHealthChecker,
                        final ConfigurationProvider configurationProvider) {
        final KafkaEventProducer kafkaEventProducer = new KafkaEventProducer(producer, convention, kafkaHealthChecker);

        List<ElasticSearchIndexed> indexedServices = Stream.of(
                        graphService, entitySearchService, systemMetadataService, timeseriesAspectService)
                .filter(service -> service instanceof ElasticSearchIndexed)
                .map(service -> (ElasticSearchIndexed) service)
                .collect(Collectors.toList());

        _steps = buildSteps(indexedServices, baseElasticSearchComponents, kafkaEventProducer, gitVersion, configurationProvider);
    }

    @Override
    public String id() {
      return "BuildIndices";
    }

    @Override
    public List<UpgradeStep> steps() {
      return _steps;
    }

    private List<UpgradeStep> buildSteps(final List<ElasticSearchIndexed> indexedServices,
        final BaseElasticSearchComponentsFactory.BaseElasticSearchComponents baseElasticSearchComponents,
        final KafkaEventProducer eventProducer, final GitVersion gitVersion, final ConfigurationProvider configurationProvider) {

      final List<UpgradeStep> steps = new ArrayList<>();
      // Disable ES write mode/change refresh rate and clone indices
      steps.add(new PreConfigureESStep(baseElasticSearchComponents, indexedServices, configurationProvider));
      // Configure graphService, entitySearchService, systemMetadataService, timeseriesAspectService
      steps.add(new BuildIndicesStep(indexedServices));
      // Reset configuration (and delete clones? Or just do this regularly? Or delete clone in pre-configure step if it already exists?
      steps.add(new PostBuildIndicesStep(baseElasticSearchComponents, indexedServices, eventProducer, gitVersion));
      return steps;
    }

    @Override
    public List<UpgradeCleanupStep> cleanupSteps() {
      return ImmutableList.of();
    }

}
