package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.graph.Neo4jGraphService;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class GraphServiceFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Nonnull
    @DependsOn({"neo4jDriver"})
    @Bean(name = "graphService")
    protected GraphService createInstance() {
        return new Neo4jGraphService(applicationContext.getBean(Driver.class));
    }
}
