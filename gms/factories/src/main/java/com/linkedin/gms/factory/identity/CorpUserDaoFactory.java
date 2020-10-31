package com.linkedin.gms.factory.identity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.metadata.aspect.CorpUserAspect;
import com.linkedin.metadata.dao.BaseLocalDAO;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.CorpUserSnapshot;
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
public class CorpUserDaoFactory {
  @Autowired
  ApplicationContext applicationContext;

  @Bean(name = "corpUserDao")
  @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
  @Nonnull
  protected BaseLocalDAO<CorpUserAspect, CorpuserUrn> createInstance() {
    KafkaMetadataEventProducer<CorpUserSnapshot, CorpUserAspect, CorpuserUrn> producer =
        new KafkaMetadataEventProducer(CorpUserSnapshot.class, CorpUserAspect.class,
            applicationContext.getBean(Producer.class), applicationContext.getBean(TopicConvention.class));
    return new EbeanLocalDAO<>(CorpUserAspect.class, producer, applicationContext.getBean(ServerConfig.class),
        CorpuserUrn.class);
  }
}