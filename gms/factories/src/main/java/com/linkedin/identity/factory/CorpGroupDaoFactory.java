package com.linkedin.identity.factory;

import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.metadata.aspect.CorpGroupAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;
import com.linkedin.metadata.snapshot.CorpGroupSnapshot;
import io.ebean.config.ServerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class CorpGroupDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "corpGroupDao")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
    @Nonnull
    protected EbeanLocalDAO createInstance() {
        KafkaMetadataEventProducer<CorpGroupSnapshot, CorpGroupAspect, CorpGroupUrn> producer =
                new KafkaMetadataEventProducer(CorpGroupSnapshot.class, CorpGroupAspect.class,
                        applicationContext.getBean(Producer.class));
        return new EbeanLocalDAO<>(CorpGroupAspect.class, producer, applicationContext.getBean(ServerConfig.class));
    }
}