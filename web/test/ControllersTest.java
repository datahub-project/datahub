
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import controllers.api.v1.Dataset;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import play.Logger;
import play.libs.Json;

import play.mvc.Http;
import play.mvc.Result;
import play.test.FakeApplication;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static play.test.Helpers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class ControllersTest {

    public static FakeApplication app;
    private final Http.Request request = mock(Http.Request.class);
    public static String DB_DEFAULT_DRIVER = "db.default.driver";
    public static String DB_DEFAULT_URL = "db.default.url";
    public static String DB_DEFAULT_USER = "db.default.user";
    public static String DB_DEFAULT_PASSWORD = "db.default.password";
    public static String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";
    public static String DATABASE_WHEREHOWS_OPENSOURCE_USER_NAME = "wherehows";
    public static String DATABASE_WHEREHOWS_OPENSOURCE_USER_PASSWORD = "wherehows";
    public static String DATABASE_WHEREHOWS_OPENSOURCE_URL = "jdbc:mysql://localhost/wherehows";
    public static Integer DB_ID = 65;

    @BeforeClass
    public static void startApp() {
        HashMap<String, String> mysql = new HashMap<String, String>();
        mysql.put(DB_DEFAULT_DRIVER, MYSQL_DRIVER_CLASS);
        mysql.put(DB_DEFAULT_URL, DATABASE_WHEREHOWS_OPENSOURCE_URL);
        mysql.put(DB_DEFAULT_USER, DATABASE_WHEREHOWS_OPENSOURCE_USER_NAME);
        mysql.put(DB_DEFAULT_PASSWORD, DATABASE_WHEREHOWS_OPENSOURCE_USER_PASSWORD);
        app = fakeApplication(mysql);
        start(app);
    }

    @Before
    public void setUp() throws Exception {
        Map<String, String> flashData = Collections.emptyMap();
        Map<String, Object> argData = Collections.emptyMap();
        Long id = 2L;
        play.api.mvc.RequestHeader header = mock(play.api.mvc.RequestHeader.class);
        Http.Context context = new Http.Context(id, header, request, flashData, flashData, argData);
        Http.Context.current.set(context);
    }

    @Ignore("need config") @Test
    public void testDataset()
    {
        Result result = controllers.api.v1.Dataset.getPagedDatasets();
        assertEquals(status(result), OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertTrue(node.isContainerNode());
        assertEquals(node.get("status").asText(), "ok");
        JsonNode dataNode = node.get("data");
        assertTrue(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertTrue(count > 0);
        JsonNode datasetsNode = dataNode.get("datasets");
        assertTrue(datasetsNode.isArray());
        JsonNode firstDatasetNode = ((ArrayNode) datasetsNode).get(0);
        assertTrue(firstDatasetNode.isContainerNode());
        Integer datasetId = firstDatasetNode.get("id").asInt();
        String name = firstDatasetNode.get("name").asText();
        assertTrue(datasetId > 0);

        result = controllers.api.v1.Dataset.getDatasetByID(datasetId);
        assertEquals(status(result), OK);
        JsonNode datasetNode = Json.parse(contentAsString(result));
        assertTrue(datasetNode.isContainerNode());
        assertEquals(datasetNode.get("status").asText(), "ok");
        JsonNode detailNode = datasetNode.get("dataset");
        assertTrue(detailNode.isContainerNode());
        String datasetName = detailNode.get("name").asText();
        assertEquals(datasetName, name);

        result = controllers.api.v1.Dataset.getDatasetColumnsByID(datasetId);
        assertEquals(status(result), OK);
        JsonNode columnsNode = Json.parse(contentAsString(result));
        assertTrue(columnsNode.isContainerNode());
        //assertEquals(columnsNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getDatasetPropertiesByID(datasetId);
        assertEquals(status(result), OK);
        JsonNode propertiesNode = Json.parse(contentAsString(result));
        assertTrue(propertiesNode.isContainerNode());
        assertEquals(propertiesNode.get("status"), "ok");

        result = controllers.api.v1.Dataset.getDatasetImpactAnalysisByID(datasetId);
        assertEquals(status(result), OK);
        JsonNode impactsNode = Json.parse(contentAsString(result));
        assertTrue(impactsNode.isContainerNode());
        assertEquals(impactsNode.get("status"), "ok");

        result = controllers.api.v1.Dataset.getDatasetSampleDataByID(datasetId);
        assertEquals(status(result), OK);
        JsonNode sampleNode = Json.parse(contentAsString(result));
        assertTrue(sampleNode.isContainerNode());
        //assertEquals(sampleNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getDatasetInstances((long)datasetId);
        assertEquals(status(result), OK);
        JsonNode instanceNode = Json.parse(contentAsString(result));
        assertTrue(instanceNode.isContainerNode());
        assertEquals(instanceNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getDatasetVersions((long)datasetId, DB_ID);
        assertEquals(status(result), OK);
        JsonNode versionNode = Json.parse(contentAsString(result));
        assertTrue(versionNode.isContainerNode());
        assertEquals(versionNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getReferenceViews((long)datasetId);
        assertEquals(status(result), OK);
        JsonNode referenceNode = Json.parse(contentAsString(result));
        assertTrue(referenceNode.isContainerNode());
        assertEquals(referenceNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getDependViews((long)datasetId);
        assertEquals(status(result), OK);
        JsonNode dependsNode = Json.parse(contentAsString(result));
        assertTrue(dependsNode.isContainerNode());
        assertEquals(dependsNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getDatasetSchemaTextByVersion((long)datasetId, "0.0.1");
        assertEquals(status(result), OK);
        JsonNode schemaTextNode = Json.parse(contentAsString(result));
        assertTrue(schemaTextNode.isContainerNode());
        assertEquals(schemaTextNode.get("status").asText(), "ok");

        result = controllers.api.v1.Dataset.getDatasetAccess((long)datasetId);
        assertEquals(status(result), OK);
        JsonNode accessNode = Json.parse(contentAsString(result));
        assertTrue(accessNode.isContainerNode());
        assertEquals(accessNode.get("status").asText(), "ok");
    }

    /*
    @Ignore("need config") @Test
    public void testMetric()
    {
        Result result = controllers.api.v1.Metric.getPagedMetrics();
        assertThat(status(result)).isEqualTo(OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertThat(node.isContainerNode());
        assertThat(node.get("status").asText()).isEqualTo("ok");
        JsonNode dataNode = node.get("data");
        assertThat(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertThat(count > 0);
        JsonNode metricsNode = dataNode.get("metrics");
        assertThat(metricsNode.isArray());
        JsonNode firstMetricNode = ((ArrayNode) metricsNode).get(0);
        assertThat(firstMetricNode.isContainerNode());
        int metricId = firstMetricNode.get("id").asInt();
        String name = firstMetricNode.get("dashboardName").asText();
        assertThat(metricId > 0);

        result = controllers.api.v1.Metric.getMetricByID(metricId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode metricNode = Json.parse(contentAsString(result));
        assertThat(metricNode.isContainerNode());
        assertThat(metricNode.get("status").asText()).isEqualTo("ok");
        JsonNode detailNode = metricNode.get("metric");
        assertThat(detailNode.isContainerNode());
        String dashboardName = detailNode.get("dashboardName").asText();
        assertThat(dashboardName.equals(name));
    }
    */

    @Ignore("need config") @Test
    public void testFlow()
    {
        Result result = controllers.api.v1.Flow.getPagedProjects("AZKABAN-SAMPLE");
        assertEquals(status(result), OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertTrue(node.isContainerNode());
        assertEquals(node.get("status").asText(), "ok");
        JsonNode dataNode = node.get("data");
        assertTrue(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertTrue(count > 0);
        JsonNode projectsNode = dataNode.get("projects");
        assertTrue(projectsNode.isArray());
        JsonNode firstProjectNode = ((ArrayNode) projectsNode).get(0);
        assertTrue(firstProjectNode.isContainerNode());
        String name = firstProjectNode.get("name").asText();

        result = controllers.api.v1.Flow.getPagedFlows("AZKABAN-SAMPLE", name);
        assertEquals(status(result), OK);
    }

    @Ignore("need config") @Test
    public void testScriptFinder()
    {
        Result result = controllers.api.v1.ScriptFinder.getScripts();
        assertEquals(status(result), OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertTrue(node.isContainerNode());
        assertEquals(node.get("status").asText(), "ok");
        JsonNode dataNode = node.get("data");
        assertTrue(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertTrue(count > 0);
    }

    @Ignore("need config") @Test
    public void testSchemaHistory()
    {
        Result result = controllers.api.v1.SchemaHistory.getPagedDatasets();
        assertEquals(status(result), OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertTrue(node.isContainerNode());
        assertEquals(node.get("status").asText(), "ok");
        JsonNode dataNode = node.get("data");
        assertTrue(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertTrue(count > 0);

        JsonNode datasetsNode = dataNode.get("datasets");
        assertTrue(datasetsNode.isArray());
        JsonNode firstDatasetNode = ((ArrayNode) datasetsNode).get(0);
        assertTrue(firstDatasetNode.isContainerNode());
        int datasetId = firstDatasetNode.get("id").asInt();
        assertTrue(datasetId > 0);

        result = controllers.api.v1.SchemaHistory.getSchemaHistory(datasetId);
        assertEquals(status(result), OK);
        JsonNode historyNode = Json.parse(contentAsString(result));
        assertTrue(historyNode.isContainerNode());
        assertEquals(historyNode.get("status").asText(), "ok");
        JsonNode historyDataNode = node.get("data");
        assertTrue(historyDataNode.isArray());
    }

    @AfterClass
    public static void stopApp() {
        stop(app);
    }
}
