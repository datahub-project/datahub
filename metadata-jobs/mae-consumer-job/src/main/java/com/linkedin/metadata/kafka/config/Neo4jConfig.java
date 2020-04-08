package com.linkedin.metadata.kafka.config;

import org.neo4j.driver.v1.Driver;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.linkedin.common.factory.Neo4jDriverFactory;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpGroupUrnCoercer;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.CorpuserUrnCoercer;
import com.linkedin.common.urn.DataPlatformUrn;
import com.linkedin.common.urn.DataPlatformUrnCoercer;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.DatasetUrnCoercer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnCoercer;
import com.linkedin.data.template.Custom;
import com.linkedin.metadata.dao.internal.BaseGraphWriterDAO;
import com.linkedin.metadata.dao.internal.Neo4jGraphWriterDAO;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@Import({Neo4jDriverFactory.class})
@RequiredArgsConstructor
public class Neo4jConfig {

    @Value("${DEFAULT_NEO4J_RETRIES:10}")
    private int neo4jRetries;

    private final Driver neo4jDriver;

    @Bean
     public BaseGraphWriterDAO graphWriterDAO() {
        BaseGraphWriterDAO graphWriterDAO = null;
        int count = 0;
        while (graphWriterDAO == null && count < neo4jRetries) {
            try {
                count++;
                graphWriterDAO = new Neo4jGraphWriterDAO(neo4jDriver);
            } catch (Exception e) {
                log.error("Unable to initialize Neo4j, retrying in 1 second.", e);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {

                }
            }
        }
        Custom.registerCoercer(new DataPlatformUrnCoercer(), DataPlatformUrn.class);
        Custom.registerCoercer(new UrnCoercer(), Urn.class);
        Custom.registerCoercer(new CorpGroupUrnCoercer(), CorpGroupUrn.class);
        Custom.registerCoercer(new CorpuserUrnCoercer(), CorpuserUrn.class);
        Custom.registerCoercer(new DatasetUrnCoercer(), DatasetUrn.class);
        log.info("Neo4jDriver built successfully");

        return graphWriterDAO;
    }
}
