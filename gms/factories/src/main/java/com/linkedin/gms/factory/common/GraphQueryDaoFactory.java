package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.Neo4jGraphClient;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class GraphQueryDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Nonnull
    @DependsOn({"neo4jDriver"})
    @Bean(name = "neo4jGraphClient")
    protected GraphClient createInstance() {
        return new Neo4jGraphClient(applicationContext.getBean(Driver.class));
    }
}
