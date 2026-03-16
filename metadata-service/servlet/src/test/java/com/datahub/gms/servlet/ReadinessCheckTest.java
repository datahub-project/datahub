package com.datahub.gms.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ebean.Database;
import io.ebean.SqlQuery;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.WriteListener;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.Map;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

@SpringBootTest()
public class ReadinessCheckTest {

    private AdminClient kafkaAdmin;
    private RestHighLevelClient elasticClient;
    private Database database;
    private Driver neo4jDriver;

    private HttpServletRequest request;
    private HttpServletResponse response;

    private ByteArrayOutputStream responseOutput;

    private ReadinessCheck readinessCheck;

    @BeforeMethod
    public void setup() throws Exception {

        kafkaAdmin = mock(AdminClient.class);
        elasticClient = mock(RestHighLevelClient.class);
        database = mock(Database.class);
        neo4jDriver = mock(Driver.class);

        request = mock(HttpServletRequest.class);
        response = mock(HttpServletResponse.class);

        responseOutput = new ByteArrayOutputStream();

        ServletOutputStream servletOutputStream =
                new ServletOutputStream() {
                    @Override
                    public void write(int b) {
                        responseOutput.write(b);
                    }

                    @Override
                    public boolean isReady() {
                        return true;
                    }

                    @Override
                    public void setWriteListener(WriteListener writeListener) {}
                };

        when(response.getOutputStream()).thenReturn(servletOutputStream);

        readinessCheck = new ReadinessCheck(kafkaAdmin, elasticClient, database, neo4jDriver);
    }

    private void mockKafkaUp() {

        DescribeClusterResult result = mock(DescribeClusterResult.class);

        when(result.clusterId()).thenReturn(KafkaFuture.completedFuture("clusterId"));

        when(kafkaAdmin.describeCluster()).thenReturn(result);
    }

    private void mockMysqlUp() {

        SqlQuery query = mock(SqlQuery.class);

        when(database.sqlQuery("select 1")).thenReturn(query);

        when(query.findOne()).thenReturn(mock(io.ebean.SqlRow.class));
    }

    private void mockElasticUp() throws Exception {

        when(elasticClient.ping(RequestOptions.DEFAULT)).thenReturn(true);
    }

    private void mockNeo4jUp() {

        Session session = mock(Session.class);
        Result result = mock(Result.class);

        when(neo4jDriver.session()).thenReturn(session);

        when(session.run("RETURN 1")).thenReturn(result);
    }

    @Test
    public void testAllServicesUp() throws Exception {

        mockKafkaUp();
        mockMysqlUp();
        mockElasticUp();
        mockNeo4jUp();

        readinessCheck.doGet(request, response);

        verify(response).setStatus(HttpServletResponse.SC_OK);

        Map<String, String> result =
                new ObjectMapper().readValue(responseOutput.toByteArray(), Map.class);

        assertEquals(result.get("kafka"), "UP");
        assertEquals(result.get("mysql"), "UP");
        assertEquals(result.get("elasticsearch"), "UP");
        assertEquals(result.get("neo4j"), "UP");
    }

    @Test
    public void testKafkaDown() throws Exception {

        when(kafkaAdmin.describeCluster()).thenThrow(new RuntimeException("Kafka failure"));

        mockMysqlUp();
        mockElasticUp();
        mockNeo4jUp();

        readinessCheck.doGet(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);

        Map<String, String> result =
                new ObjectMapper().readValue(responseOutput.toByteArray(), Map.class);

        assertEquals(result.get("kafka"), "DOWN");
    }

    @Test
    public void testMysqlDown() throws Exception {

        mockKafkaUp();

        SqlQuery query = mock(SqlQuery.class);

        when(database.sqlQuery("select 1")).thenReturn(query);

        when(query.findOne()).thenReturn(null);

        mockElasticUp();
        mockNeo4jUp();

        readinessCheck.doGet(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testElasticDown() throws Exception {

        mockKafkaUp();
        mockMysqlUp();

        when(elasticClient.ping(RequestOptions.DEFAULT)).thenThrow(new RuntimeException());

        mockNeo4jUp();

        readinessCheck.doGet(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testNeo4jDown() throws Exception {

        mockKafkaUp();
        mockMysqlUp();
        mockElasticUp();

        when(neo4jDriver.session()).thenThrow(new RuntimeException());

        readinessCheck.doGet(request, response);

        verify(response).setStatus(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
    }

    @Test
    public void testDestroyExecutor() {

        readinessCheck.destroy();

        assertTrue(true);
    }
}
