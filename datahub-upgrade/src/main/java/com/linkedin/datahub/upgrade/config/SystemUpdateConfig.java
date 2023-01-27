package com.linkedin.datahub.upgrade.config;

import com.linkedin.datahub.upgrade.system.SystemUpdate;
import com.linkedin.datahub.upgrade.system.elasticsearch.BuildIndices;
import com.linkedin.datahub.upgrade.system.elasticsearch.CleanIndices;
import com.linkedin.metadata.dao.producer.KafkaHealthChecker;
import com.linkedin.metadata.version.GitVersion;
import com.linkedin.mxe.TopicConvention;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Configuration
public class SystemUpdateConfig {
  @Bean(name = "systemUpdate")
  public SystemUpdate systemUpdate(final BuildIndices buildIndices, final CleanIndices cleanIndices,
                                   final Producer<String, ? extends IndexedRecord> producer,
                                   final TopicConvention convention, final GitVersion gitVersion,
                                   final KafkaHealthChecker kafkaHealthChecker, @Qualifier("revision") String revision) {

    String version = String.format("%s-%s", gitVersion.getVersion(), revision);
    return new SystemUpdate(buildIndices, cleanIndices, producer, convention, version, kafkaHealthChecker);
  }

  @Value("#{systemEnvironment['DATAHUB_REVISION'] ?: '0'}")
  private String revision;

  @Bean(name = "revision")
  public String getRevision() {
    return revision;
  }
}
