package com.linkedin.datahub.upgrade.system;

import com.linkedin.datahub.upgrade.Upgrade;
import com.linkedin.datahub.upgrade.UpgradeCleanupStep;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.datahub.upgrade.system.elasticsearch.CleanIndices;
import com.linkedin.datahub.upgrade.system.elasticsearch.steps.DataHubStartupStep;
import com.linkedin.metadata.dao.producer.KafkaEventProducer;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.mxe.TopicConvention;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


@Slf4j
public class SystemUpdate implements Upgrade {
    private final List<Upgrade> _preStartupUpgrades;
    private final List<Upgrade> _postStartupUpgrades;
    private final List<UpgradeStep> _steps;

    public SystemUpdate(final BuildIndices buildIndicesJob, final CleanIndices cleanIndicesJob,
                        final Producer<String, ? extends IndexedRecord> producer,
                        final TopicConvention convention, final String version, final KafkaHealthChecker kafkaHealthChecker) {
        final KafkaEventProducer kafkaEventProducer = new KafkaEventProducer(producer, convention, kafkaHealthChecker);

        _preStartupUpgrades = List.of(buildIndicesJob);
        _steps = List.of(new DataHubStartupStep(kafkaEventProducer, version));
        _postStartupUpgrades = List.of(cleanIndicesJob);
    }

    @Override
    public String id() {
      return "SystemUpdate";
    }

    @Override
    public List<UpgradeStep> steps() {
        return Stream.concat(Stream.concat(
                _preStartupUpgrades.stream().flatMap(up -> up.steps().stream()),
                _steps.stream()),
                _postStartupUpgrades.stream().flatMap(up -> up.steps().stream()))
                .collect(Collectors.toList());
    }

    @Override
    public List<UpgradeCleanupStep> cleanupSteps() {
        return Stream.concat(
                _preStartupUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()),
                _postStartupUpgrades.stream().flatMap(up -> up.cleanupSteps().stream()))
                .collect(Collectors.toList());
    }
}
