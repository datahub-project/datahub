package com.linkedin.metadata.kafka.boot;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.kafka.config.MetadataChangeLogProcessorCondition;
import com.linkedin.metadata.kafka.elasticsearch.indices.BuildIndicesKafkaListener;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.context.WebApplicationContext;


/**
 * Responsible for coordinating starting steps that happen before the application starts up.
 */
@Slf4j
@Component
@Conditional(MetadataChangeLogProcessorCondition.class)
public class ApplicationStartupListener implements ApplicationListener<ContextRefreshedEvent> {

  private static final String ROOT_WEB_APPLICATION_CONTEXT_ID = String.format("%s:", WebApplicationContext.class.getName());

  private final BuildIndicesKafkaListener _buildIndicesKafkaListener;
  private final ConfigurationProvider _configurationProvider;

  public ApplicationStartupListener(
      @Qualifier("buildIndicesKafkaListener") BuildIndicesKafkaListener buildIndicesKafkaListener,
      ConfigurationProvider configurationProvider) {
    _buildIndicesKafkaListener = buildIndicesKafkaListener;
    _configurationProvider = configurationProvider;
  }

  @Override
  public void onApplicationEvent(@Nonnull ContextRefreshedEvent event) {
    if (ROOT_WEB_APPLICATION_CONTEXT_ID.equals(event.getApplicationContext().getId())
        && _configurationProvider.getElasticSearch().getBuildIndices().isWaitForBuildIndices()) {
      _buildIndicesKafkaListener.waitForBootstrap();
    }
  }
}
