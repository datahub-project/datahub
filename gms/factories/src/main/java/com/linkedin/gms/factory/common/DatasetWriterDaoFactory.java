package com.linkedin.gms.factory.common;

import com.linkedin.metadata.dao.Neo4jQueryDAO;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.Nonnull;

@Configuration
public class DatasetWriterDaoFactory {
    @Autowired
    ApplicationContext applicationContext;

    @Nonnull
    @DependsOn({"neo4jDriver"})
    @Bean(name = "graphWriterDao")
    protected Neo4jGraphWriterDAO createInstance() {
        return new Neo4jGraphWriterDAO(applicationContext.getBean(Driver.class));
    }
}
