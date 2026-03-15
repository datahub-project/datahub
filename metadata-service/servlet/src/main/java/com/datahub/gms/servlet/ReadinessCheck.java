package com.datahub.gms.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ebean.Database;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.admin.AdminClient;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


public class ReadinessCheck extends HttpServlet {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ReadinessCheck.class);

    private final AdminClient kafkaAdmin;
    private final RestHighLevelClient elasticClient;
    private final Database database;
    private final Driver neo4jDriver;

    public ReadinessCheck(
            AdminClient kafkaAdmin,
            RestHighLevelClient elasticClient,
            Database database,
            Driver neo4jDriver) {
        this.kafkaAdmin = kafkaAdmin;
        this.elasticClient = elasticClient;
        this.database = database;
        this.neo4jDriver = neo4jDriver;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        Map<String, String> result = new LinkedHashMap<>();

        boolean kafkaUp = checkKafka();
        boolean mysqlUp = checkMySql();
        boolean elasticUp = checkElastic();
        boolean neo4jUp = checkNeo4j();

        boolean allUp = kafkaUp && mysqlUp && elasticUp && neo4jUp;

        result.put("kafka", kafkaUp ? "UP" : "DOWN");
        result.put("mysql", mysqlUp ? "UP" : "DOWN");
        result.put("elasticsearch", elasticUp ? "UP" : "DOWN");
        result.put("neo4j", neo4jUp ? "UP" : "DOWN");

        resp.setStatus(
                allUp
                        ? HttpServletResponse.SC_OK
                        : HttpServletResponse.SC_SERVICE_UNAVAILABLE);

        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json");

        MAPPER.writeValue(resp.getOutputStream(), result);
    }

    private boolean checkKafka() {
        try {
            kafkaAdmin.describeCluster()
                    .clusterId()
                    .get(2, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            log.error("Kafka readiness check failed", e);
            return false;
        }
    }

    private boolean checkMySql() {
        try {
            database.sqlQuery("select 1").findOne();
            return true;
        } catch (Exception e) {
            log.error("MySQL readiness check failed", e);
            return false;
        }
    }

    private boolean checkElastic() {
        try {
            return elasticClient.ping(RequestOptions.DEFAULT);
        } catch (Exception e) {
            log.error("Elastic readiness check failed", e);
            return false;
        }
    }

    private boolean checkNeo4j() {
        try (Session session = neo4jDriver.session()) {
            session.run("RETURN 1").consume();
            return true;
        } catch (Exception e) {
            log.error("Neo4j readiness check failed", e);
            return false;
        }
    }
}
