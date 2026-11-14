package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.kafka.PolicyChangeTopic;
import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.metadata.EventUtils;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.ObjectMapperContext;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class SchemaRegistryServiceFactory {

  @Bean(name = "eventSchemaData")
  @Nonnull
  protected EventSchemaData eventSchemaData() {
    return new EventSchemaData(ObjectMapperContext.DEFAULT.getYamlMapper());
  }

  @Bean(name = "schemaRegistryService")
  @Nonnull
  @DependsOn({TopicConventionFactory.TOPIC_CONVENTION_BEAN, "eventSchemaData"})
  protected SchemaRegistryService schemaRegistryService(
      TopicConvention convention,
      EventSchemaData eventSchemaData,
      ConfigurationProvider configurationProvider) {
    final SchemaRegistryServiceImpl service =
        new SchemaRegistryServiceImpl(convention, eventSchemaData);
    // The optional policy change topic carries change logs (POLICY_CACHE_BROADCAST_ENABLED).
    PolicyChangeTopic.enabledTopicName(configurationProvider)
        .ifPresent(
            topic ->
                service.registerTopicSchema(topic, EventUtils.METADATA_CHANGE_LOG_SCHEMA_NAME));
    return service;
  }
}
