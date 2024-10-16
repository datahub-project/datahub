package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.mxe.TopicConvention;
import javax.annotation.Nonnull;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class SchemaRegistryServiceFactory {
  @Bean(name = "schemaRegistryService")
  @Nonnull
  @DependsOn({TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  protected SchemaRegistryService schemaRegistryService(TopicConvention convention) {
    return new SchemaRegistryServiceImpl(convention);
  }
}
