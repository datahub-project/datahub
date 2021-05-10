package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.dao.EntityDao;
import com.linkedin.metadata.kafka.EntityEventProducer;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
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
public class EntityDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "entityDao")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
    @Nonnull
    protected EntityDao createInstance() {

        final EntityRegistry registry = new SnapshotEntityRegistry();

        final EntityEventProducer producer = new EntityEventProducer(
                // Minimum, pass in an entity registry.
                registry,
                applicationContext.getBean(Producer.class),
                applicationContext.getBean(TopicConvention.class));

        return new EntityDao(
                registry,
                producer,
                applicationContext.getBean(ServerConfig.class));
    }
}