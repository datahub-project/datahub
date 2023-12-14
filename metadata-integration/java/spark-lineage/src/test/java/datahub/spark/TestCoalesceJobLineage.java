package datahub.spark;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;

import com.linkedin.common.FabricType;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.socket.PortFactory;
import org.mockserver.verify.VerificationTimes;

public class TestCoalesceJobLineage {
  private static final boolean MOCK_GMS = Boolean.valueOf("true");
  // if false, MCPs get written to real GMS server (see GMS_PORT)
  private static final boolean VERIFY_EXPECTED = MOCK_GMS && Boolean.valueOf("true");
  // if false, "expected" JSONs are overwritten.

  private static final String APP_NAME = "sparkCoalesceTestApp";

  private static final String TEST_RELATIVE_PATH = "../";
  private static final String RESOURCE_DIR = "src/test/resources";
  private static final String DATA_DIR = TEST_RELATIVE_PATH + RESOURCE_DIR + "/data";
  private static final String WAREHOUSE_LOC = DATA_DIR + "/hive/warehouse/coalesce";
  private static final String TEST_DB = "sparktestdb";

  private static final String MASTER = "local[1]";

  private static final int MOCK_PORT = PortFactory.findFreePort();
  private static final int GMS_PORT = MOCK_GMS ? MOCK_PORT : 8080;

  private static final String EXPECTED_JSON_ROOT = "src/test/resources/expected/";

  private static final FabricType DATASET_ENV = FabricType.DEV;
  private static final String PIPELINE_PLATFORM_INSTANCE = "test_machine";
  private static final String DATASET_PLATFORM_INSTANCE = "test_dev_dataset";

  private static SparkSession spark;
  private static Properties jdbcConnnProperties;
  private static ClientAndServer mockServer;

  @Rule
  public TestRule mockServerWatcher =
      new TestWatcher() {

        @Override
        protected void finished(Description description) {
          if (!VERIFY_EXPECTED) {
            return;
          }
          verifyTestScenario(description.getMethodName());
          clear();
          super.finished(description);
        }
      };

  private static String addLocalPath(String s) {
    return s.replaceAll(
        "file:/" + RESOURCE_DIR, "file://" + Paths.get(RESOURCE_DIR).toAbsolutePath().toString());
  }

  public static void resetBaseExpectations() {
    mockServer
        .when(
            request()
                .withMethod("GET")
                .withPath("/config")
                .withHeader("Content-type", "application/json"),
            Times.unlimited())
        .respond(org.mockserver.model.HttpResponse.response().withBody("{\"noCode\": true }"));
    mockServer
        .when(
            request()
                .withMethod("POST")
                .withPath("/aspects")
                .withQueryStringParameter("action", "ingestProposal"),
            Times.unlimited())
        .respond(HttpResponse.response().withStatusCode(200));
  }

  @BeforeClass
  public static void initMockServer() {
    mockServer = startClientAndServer(GMS_PORT);
    resetBaseExpectations();
  }

  public static void verifyTestScenario(String testName) {
    String expectationFileName = testName + ".json";
    try {
      List<String> expected =
          Files.readAllLines(Paths.get(EXPECTED_JSON_ROOT, expectationFileName).toAbsolutePath());
      for (String content : expected) {
        String swappedContent = addLocalPath(content);
        mockServer.verify(
            request()
                .withMethod("POST")
                .withPath("/aspects")
                .withQueryStringParameter("action", "ingestProposal")
                .withBody(new JsonBody(swappedContent)),
            VerificationTimes.atLeast(1));
      }
    } catch (IOException ioe) {
      throw new RuntimeException("failed to read expectations", ioe);
    }
  }

  @Before
  public void setup() {

    resetBaseExpectations();
    System.setProperty("user.dir", Paths.get("coalesce-test").toAbsolutePath().toString());
    spark =
        SparkSession.builder()
            .appName(APP_NAME)
            .config("spark.master", MASTER)
            .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
            .config("spark.datahub.rest.server", "http://localhost:" + mockServer.getPort())
            .config("spark.datahub.metadata.pipeline.platformInstance", PIPELINE_PLATFORM_INSTANCE)
            .config("spark.datahub.metadata.dataset.platformInstance", DATASET_PLATFORM_INSTANCE)
            .config("spark.datahub.metadata.dataset.env", DATASET_ENV.name())
            .config("spark.datahub.coalesce_jobs", "true")
            .config(
                "spark.datahub.parent.datajob_urn",
                "urn:li:dataJob:(urn:li:dataFlow:(airflow,datahub_analytics_refresh,prod),load_dashboard_info_to_snowflake)")
            .config("spark.sql.warehouse.dir", new File(WAREHOUSE_LOC).getAbsolutePath())
            .enableHiveSupport()
            .getOrCreate();

    spark.sql("drop database if exists " + TEST_DB + " cascade");
    spark.sql("create database " + TEST_DB);
  }

  private static void clear() {
    mockServer.clear(
        request()
            .withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal"));
  }

  @After
  public void afterTearDown() throws Exception {
    spark.stop();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    mockServer.stop();
  }

  private static String tbl(String tbl) {
    return TEST_DB + "." + tbl;
  }

  public static void verify(int numRequests) {
    if (!VERIFY_EXPECTED) {
      return;
    }
    mockServer.verify(
        request()
            .withMethod("POST")
            .withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal"),
        VerificationTimes.exactly(numRequests));
  }

  @Test
  public void testHiveInHiveOutCoalesce() throws Exception {
    Dataset<Row> df1 =
        spark
            .read()
            .option("header", "true")
            .csv(new File(DATA_DIR + "/in1.csv").getAbsolutePath())
            .withColumnRenamed("c1", "a")
            .withColumnRenamed("c2", "b");

    Dataset<Row> df2 =
        spark
            .read()
            .option("header", "true")
            .csv(new File(DATA_DIR + "/in2.csv").getAbsolutePath())
            .withColumnRenamed("c1", "c")
            .withColumnRenamed("c2", "d");

    df1.createOrReplaceTempView("v1");
    df2.createOrReplaceTempView("v2");

    // CreateHiveTableAsSelectCommand
    spark.sql(
        "create table "
            + tbl("foo_coalesce")
            + " as "
            + "(select v1.a, v1.b, v2.c, v2.d from v1 join v2 on v1.id = v2.id)");

    // CreateHiveTableAsSelectCommand
    spark.sql(
        "create table " + tbl("hivetab") + " as " + "(select * from " + tbl("foo_coalesce") + ")");

    // InsertIntoHiveTable
    spark.sql("insert into " + tbl("hivetab") + " (select * from " + tbl("foo_coalesce") + ")");

    Dataset<Row> df = spark.sql("select * from " + tbl("foo_coalesce"));

    // InsertIntoHiveTable
    df.write().insertInto(tbl("hivetab"));
    Thread.sleep(5000);
  }
}
