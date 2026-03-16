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
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class ReadinessCheck extends HttpServlet {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(ReadinessCheck.class);
    private static final long CHECK_TIMEOUT_SECONDS = 2L;

    private final AdminClient kafkaAdmin;
    private final RestHighLevelClient elasticClient;
    private final Database database;
    private final Driver neo4jDriver;
    private final ExecutorService healthCheckExecutor = Executors.newFixedThreadPool(4);

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
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        Map<String, String> result = new LinkedHashMap<>();

        CompletableFuture<Boolean> kafkaFuture = runCheck(this::checkKafka, "Kafka");
        CompletableFuture<Boolean> mysqlFuture = runCheck(this::checkMySql, "MySQL");
        CompletableFuture<Boolean> elasticFuture = runCheck(this::checkElastic, "Elastic");
        CompletableFuture<Boolean> neo4jFuture = runCheck(this::checkNeo4j, "Neo4j");

        boolean kafkaUp = kafkaFuture.join();
        boolean mysqlUp = mysqlFuture.join();
        boolean elasticUp = elasticFuture.join();
        boolean neo4jUp = neo4jFuture.join();

        boolean allUp = kafkaUp && mysqlUp && elasticUp && neo4jUp;

        result.put("kafka", kafkaUp ? "UP" : "DOWN");
        result.put("mysql", mysqlUp ? "UP" : "DOWN");
        result.put("elasticsearch", elasticUp ? "UP" : "DOWN");
        result.put("neo4j", neo4jUp ? "UP" : "DOWN");

        resp.setStatus(allUp
                ? HttpServletResponse.SC_OK
                : HttpServletResponse.SC_SERVICE_UNAVAILABLE);
        resp.setCharacterEncoding(StandardCharsets.UTF_8.name());
        resp.setContentType("application/json");

        try {
            MAPPER.writeValue(resp.getOutputStream(), result);
            resp.getOutputStream().flush();
        } catch (IOException e) {
            log.error("Error writing the health readiness response", e);
        }
    }

    private CompletableFuture<Boolean> runCheck(Supplier<Boolean> check, String name) {
        return CompletableFuture.supplyAsync(check, healthCheckExecutor)
                .completeOnTimeout(false, CHECK_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    log.error("{} readiness check failed", name, ex);
                    return false;
                });
    }

    private boolean checkKafka() {
        try {
            kafkaAdmin.describeCluster().clusterId().get(2, TimeUnit.SECONDS);
            return true;
        } catch (Exception e) {
            log.error("Kafka readiness check failed", e);
            return false;
        }
    }

    private boolean checkMySql() {
        try {
            if (database.sqlQuery("select 1").findOne() == null) {
                throw new SQLException("No result returned from MySQL health check query");
            }
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

    @Override
    public void destroy() {
        healthCheckExecutor.shutdown();
        super.destroy();
    }
}
