package com.linkedin.gms.factory.common;

import com.linkedin.metadata.graph.Neo4jGraphDAO;
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
    @Bean(name = "graphQueryDao")
    protected Neo4jGraphDAO createInstance() {
        return new Neo4jGraphDAO(applicationContext.getBean(Driver.class));
    }
}
