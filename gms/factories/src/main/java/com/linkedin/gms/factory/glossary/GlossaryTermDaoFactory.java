package com.linkedin.gms.factory.glossary;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.metadata.aspect.GlossaryTermAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.GlossaryTermSnapshot;
import com.linkedin.mxe.TopicConvention;
import io.ebean.config.ServerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;


@Configuration
public class GlossaryTermDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "glossaryTermDao")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  @Nonnull
  protected BaseLocalDAO<GlossaryTermAspect, GlossaryTermUrn> createInstance() {
    KafkaMetadataEventProducer<GlossaryTermSnapshot, GlossaryTermAspect, GlossaryTermUrn> producer =
        new KafkaMetadataEventProducer(GlossaryTermSnapshot.class, GlossaryTermAspect.class,
            applicationContext.getBean(Producer.class), applicationContext.getBean(TopicConvention.class));
    return new EbeanLocalDAO<>(GlossaryTermAspect.class, producer, applicationContext.getBean(ServerConfig.class),
        GlossaryTermUrn.class);
  }
}