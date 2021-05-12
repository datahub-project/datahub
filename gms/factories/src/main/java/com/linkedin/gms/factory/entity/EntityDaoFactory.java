package com.linkedin.gms.factory.entity;

import com.linkedin.gms.factory.common.TopicConventionFactory;
import com.linkedin.metadata.dao.EntityDao;
import com.linkedin.metadata.kafka.EntityEventProducer;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import com.linkedin.mxe.TopicConvention;
import io.ebean.config.ServerConfig;
import javax.annotation.Nonnull;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;


@Configuration
public class EntityDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Bean(name = "entityDao")
    @DependsOn({"gmsEbeanServiceConfig", "kafkaEventProducer", TopicConventionFactory.TOPIC_CONVENTION_BEAN})
    @Nonnull
    protected EntityDao createInstance() {

        final EntityEventProducer producer = new EntityEventProducer(
                // Minimum, pass in an entity registry.
                SnapshotEntityRegistry.getInstance(),
                applicationContext.getBean(Producer.class),
                applicationContext.getBean(TopicConvention.class));

        return new EntityDao(
                SnapshotEntityRegistry.getInstance(),
                producer,
                applicationContext.getBean(ServerConfig.class));
    }
}