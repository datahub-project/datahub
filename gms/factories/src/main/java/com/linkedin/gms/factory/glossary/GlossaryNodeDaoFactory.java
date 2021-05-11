package com.linkedin.gms.factory.glossary;

import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.aspect.GlossaryNodeAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.GlossaryNodeSnapshot;
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
public class GlossaryNodeDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "glossaryNodeDao")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  @Nonnull
  protected BaseLocalDAO<GlossaryNodeAspect, GlossaryNodeUrn> createInstance() {
    KafkaMetadataEventProducer<GlossaryNodeSnapshot, GlossaryNodeAspect, GlossaryNodeUrn> producer =
        new KafkaMetadataEventProducer(GlossaryNodeSnapshot.class, GlossaryNodeAspect.class,
            applicationContext.getBean(Producer.class), applicationContext.getBean(TopicConvention.class));
    return new EbeanLocalDAO<>(GlossaryNodeAspect.class, producer, applicationContext.getBean(ServerConfig.class),
            GlossaryNodeUrn.class);
  }
}