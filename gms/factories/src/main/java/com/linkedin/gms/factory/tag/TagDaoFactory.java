package com.linkedin.gms.factory.tag;

import com.linkedin.common.urn.TagUrn;
import com.linkedin.metadata.aspect.TagAspect;
import com.linkedin.metadata.dao.EbeanLocalDAO;
import com.linkedin.metadata.dao.producer.KafkaMetadataEventProducer;

import com.linkedin.metadata.snapshot.TagSnapshot;
import io.ebean.config.ServerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class TagDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "tagDAO")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer"})
    @Nonnull
    protected EbeanLocalDAO createInstance() {
        KafkaMetadataEventProducer<TagSnapshot, TagAspect, TagUrn> producer =
                new KafkaMetadataEventProducer(TagSnapshot.class, TagAspect.class,
                        applicationContext.getBean(Producer.class));
        return new EbeanLocalDAO<>(TagAspect.class, producer, applicationContext.getBean(ServerConfig.class),
                TagUrn.class);
    }
}
