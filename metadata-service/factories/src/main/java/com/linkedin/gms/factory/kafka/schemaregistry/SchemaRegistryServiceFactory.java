package com.linkedin.gms.factory.kafka.schemaregistry;

import com.linkedin.gms.factory.kafka.common.TopicConventionFactory;
import com.linkedin.metadata.EventSchemaData;
import com.linkedin.metadata.registry.SchemaRegistryService;
import com.linkedin.metadata.registry.SchemaRegistryServiceImpl;
import com.linkedin.mxe.TopicConvention;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class SchemaRegistryServiceFactory {

  @Bean(name = "eventSchemaData")
  @Nonnull
  protected EventSchemaData eventSchemaData(
      @Qualifier("systemOperationContext") final OperationContext systemOpContext) {
    return new EventSchemaData(systemOpContext.getYamlMapper());
  }

  @Bean(name = "schemaRegistryService")
  @Nonnull
  @DependsOn({TopicConventionFactory.TOPIC_CONVENTION_BEAN, "eventSchemaData"})
  protected SchemaRegistryService schemaRegistryService(
      TopicConvention convention, EventSchemaData eventSchemaData) {
    return new SchemaRegistryServiceImpl(convention, eventSchemaData);
  }
}
