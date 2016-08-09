
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import controllers.api.v1.Dataset;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import play.Logger;
import play.libs.Json;

import play.mvc.Content;
import play.mvc.Http;
import play.mvc.Result;
import play.test.FakeApplication;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static play.test.Helpers.*;
import static org.fest.assertions.Assertions.*;
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
        assertThat(status(result)).isEqualTo(OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertThat(node.isContainerNode());
        assertThat(node.get("status").asText()).isEqualTo("ok");
        JsonNode dataNode = node.get("data");
        assertThat(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertThat(count > 0);
        JsonNode datasetsNode = dataNode.get("datasets");
        assertThat(datasetsNode.isArray());
        JsonNode firstDatasetNode = ((ArrayNode) datasetsNode).get(0);
        assertThat(firstDatasetNode.isContainerNode());
        Integer datasetId = firstDatasetNode.get("id").asInt();
        String name = firstDatasetNode.get("name").asText();
        assertThat(datasetId > 0);

        result = controllers.api.v1.Dataset.getDatasetByID(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode datasetNode = Json.parse(contentAsString(result));
        assertThat(datasetNode.isContainerNode());
        assertThat(datasetNode.get("status").asText()).isEqualTo("ok");
        JsonNode detailNode = datasetNode.get("dataset");
        assertThat(detailNode.isContainerNode());
        String datasetName = detailNode.get("name").asText();
        assertThat(datasetName.equals(name));

        result = controllers.api.v1.Dataset.getDatasetColumnsByID(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode columnsNode = Json.parse(contentAsString(result));
        assertThat(columnsNode.isContainerNode());
        //assertThat(columnsNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetPropertiesByID(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode propertiesNode = Json.parse(contentAsString(result));
        assertThat(propertiesNode.isContainerNode());
        assertThat(propertiesNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetImpactAnalysisByID(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode impactsNode = Json.parse(contentAsString(result));
        assertThat(impactsNode.isContainerNode());
        assertThat(impactsNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetSampleDataByID(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode sampleNode = Json.parse(contentAsString(result));
        assertThat(sampleNode.isContainerNode());
        //assertThat(sampleNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetInstances(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode instanceNode = Json.parse(contentAsString(result));
        assertThat(instanceNode.isContainerNode());
        assertThat(instanceNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetVersions(datasetId, DB_ID);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode versionNode = Json.parse(contentAsString(result));
        assertThat(versionNode.isContainerNode());
        assertThat(versionNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getReferenceViews(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode referenceNode = Json.parse(contentAsString(result));
        assertThat(referenceNode.isContainerNode());
        assertThat(referenceNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDependViews(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode dependsNode = Json.parse(contentAsString(result));
        assertThat(dependsNode.isContainerNode());
        assertThat(dependsNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetSchemaTextByVersion(datasetId, '0.0.1');
        assertThat(status(result)).isEqualTo(OK);
        JsonNode schemaTextNode = Json.parse(contentAsString(result));
        assertThat(schemaTextNode.isContainerNode());
        assertThat(schemaTextNode.get("status").asText()).isEqualTo("ok");

        result = controllers.api.v1.Dataset.getDatasetAccess(datasetId)
        assertThat(status(result)).isEqualTo(OK);
        JsonNode accessNode = Json.parse(contentAsString(result));
        assertThat(accessNode.isContainerNode());
        assertThat(accessNode.get("status").asText()).isEqualTo("ok");
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
        assertThat(status(result)).isEqualTo(OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertThat(node.isContainerNode());
        assertThat(node.get("status").asText()).isEqualTo("ok");
        JsonNode dataNode = node.get("data");
        assertThat(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertThat(count > 0);
        JsonNode projectsNode = dataNode.get("projects");
        assertThat(projectsNode.isArray());
        JsonNode firstProjectNode = ((ArrayNode) projectsNode).get(0);
        assertThat(firstProjectNode.isContainerNode());
        String name = firstProjectNode.get("name").asText();

        result = controllers.api.v1.Flow.getPagedFlows("AZKABAN-SAMPLE", name);
        assertThat(status(result)).isEqualTo(OK);
    }

    @Ignore("need config") @Test
    public void testScriptFinder()
    {
        Result result = controllers.api.v1.ScriptFinder.getScripts();
        assertThat(status(result)).isEqualTo(OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertThat(node.isContainerNode());
        assertThat(node.get("status").asText()).isEqualTo("ok");
        JsonNode dataNode = node.get("data");
        assertThat(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertThat(count > 0);
    }

    @Ignore("need config") @Test
    public void testSchemaHistory()
    {
        Result result = controllers.api.v1.SchemaHistory.getPagedDatasets();
        assertThat(status(result)).isEqualTo(OK);
        JsonNode node = Json.parse(contentAsString(result));
        assertThat(node.isContainerNode());
        assertThat(node.get("status").asText()).isEqualTo("ok");
        JsonNode dataNode = node.get("data");
        assertThat(dataNode.isContainerNode());
        long count = dataNode.get("count").asLong();
        assertThat(count > 0);

        JsonNode datasetsNode = dataNode.get("datasets");
        assertThat(datasetsNode.isArray());
        JsonNode firstDatasetNode = ((ArrayNode) datasetsNode).get(0);
        assertThat(firstDatasetNode.isContainerNode());
        int datasetId = firstDatasetNode.get("id").asInt();
        assertThat(datasetId > 0);

        result = controllers.api.v1.SchemaHistory.getSchemaHistory(datasetId);
        assertThat(status(result)).isEqualTo(OK);
        JsonNode historyNode = Json.parse(contentAsString(result));
        assertThat(historyNode.isContainerNode());
        assertThat(historyNode.get("status").asText()).isEqualTo("ok");
        JsonNode historyDataNode = node.get("data");
        assertThat(historyDataNode.isArray());
    }

    @AfterClass
    public static void stopApp() {
        stop(app);
    }
}
