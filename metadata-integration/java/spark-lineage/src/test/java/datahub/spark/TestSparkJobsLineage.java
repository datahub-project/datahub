package datahub.spark;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.junit.runners.MethodSorters;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.matchers.Times;
import org.mockserver.model.HttpResponse;
import org.mockserver.model.JsonBody;
import org.mockserver.socket.PortFactory;
import org.mockserver.verify.VerificationTimes;
import org.testcontainers.containers.PostgreSQLContainer;

import com.linkedin.common.FabricType;

import datahub.spark.model.DatasetLineage;
import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageEvent;
import datahub.spark.model.LineageUtils;
import datahub.spark.model.SQLQueryExecStartEvent;
import datahub.spark.model.dataset.CatalogTableDataset;
import datahub.spark.model.dataset.HdfsPathDataset;
import datahub.spark.model.dataset.JdbcDataset;
import datahub.spark.model.dataset.SparkDataset;

//!!!! IMP  !!!!!!!!
//Add the test number before naming the test. This will ensure that tests run in specified order. 
//This is necessary to have fixed query execution numbers. Otherwise tests will fail.
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class TestSparkJobsLineage {
  private static final boolean MOCK_GMS = Boolean.valueOf("true");
  // if false, MCPs get written to real GMS server (see GMS_PORT)
  private static final boolean VERIFY_EXPECTED = MOCK_GMS && Boolean.valueOf("true");
  // if false, "expected" JSONs are overwritten.

  private static final String APP_NAME = "sparkTestApp";

  private static final String RESOURCE_DIR = "src/test/resources";
  private static final String DATA_DIR = RESOURCE_DIR + "/data";
  private static final String WAREHOUSE_LOC = DATA_DIR + "/hive/warehouse";
  private static final String TEST_DB = "sparktestdb";

  private static final String MASTER = "local";

  private static final int N = 3; // num of GMS requests per spark job

  private static final int MOCK_PORT = PortFactory.findFreePort();
  private static final int GMS_PORT = MOCK_GMS ? MOCK_PORT : 8080;

  private static final String EXPECTED_JSON_ROOT = "src/test/resources/expected/";

  private static final FabricType DATASET_ENV = FabricType.DEV;
  private static final String PIPELINE_PLATFORM_INSTANCE = "test_machine";
  private static final String DATASET_PLATFORM_INSTANCE = "test_dev_dataset";

  @ClassRule
  public static PostgreSQLContainer<?> db = new PostgreSQLContainer<>("postgres:9.6.12")
      .withDatabaseName("sparktestdb");
  private static SparkSession spark;
  private static Properties jdbcConnnProperties;
  private static DatasetLineageAccumulator acc;
  private static ClientAndServer mockServer;
  @Rule
  public TestRule mockServerWatcher = new TestWatcher() {

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
    return s.replaceAll("file:/" + RESOURCE_DIR, "file:" + Paths.get(RESOURCE_DIR).toAbsolutePath().toString());
  }

  public static void resetBaseExpectations() {
    mockServer.when(request().withMethod("GET").withPath("/config").withHeader("Content-type", "application/json"),
        Times.unlimited()).respond(org.mockserver.model.HttpResponse.response().withBody("{\"noCode\": true }"));
    mockServer
        .when(request().withMethod("POST").withPath("/aspects").withQueryStringParameter("action", "ingestProposal"),
            Times.unlimited())
        .respond(HttpResponse.response().withStatusCode(200));
  }

  public static void init() {
    mockServer = startClientAndServer(GMS_PORT);
    resetBaseExpectations();
  }

  public static void verifyTestScenario(String testName) {
    String expectationFileName = testName + ".json";
    try {
      List<String> expected = Files.readAllLines(Paths.get(EXPECTED_JSON_ROOT, expectationFileName));
      for (String content : expected) {
        String swappedContent = addLocalPath(content);
        mockServer.verify(request().withMethod("POST").withPath("/aspects")
            .withQueryStringParameter("action", "ingestProposal").withBody(new JsonBody(swappedContent)),
            VerificationTimes.atLeast(1));
      }
    } catch (IOException ioe) {
      throw new RuntimeException("failed to read expectations", ioe);
    }
  }

  public static void verify(int numRequests) {
    if (!VERIFY_EXPECTED) {
      return;
    }
    mockServer.verify(
        request().withMethod("POST").withPath("/aspects").withQueryStringParameter("action", "ingestProposal"),
        VerificationTimes.exactly(numRequests));
  }

  @BeforeClass
  public static void setup() {

    acc = new DatasetLineageAccumulator();
    LineageUtils.registerConsumer("accumulator", acc);
    init();

    spark = SparkSession.builder().appName(APP_NAME).config("spark.master", MASTER)
        .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
        .config("spark.datahub.lineage.consumerTypes", "accumulator")
        .config("spark.datahub.rest.server", "http://localhost:" + mockServer.getPort())
        .config("spark.datahub.metadata.pipeline.platformInstance", PIPELINE_PLATFORM_INSTANCE)
        .config("spark.datahub.metadata.dataset.platformInstance", DATASET_PLATFORM_INSTANCE)
        .config("spark.datahub.metadata.dataset.env", DATASET_ENV.name())
        .config("spark.sql.warehouse.dir", new File(WAREHOUSE_LOC).getAbsolutePath()).enableHiveSupport().getOrCreate();

    spark.sql("drop database if exists " + TEST_DB + " cascade");
    spark.sql("create database " + TEST_DB);
    jdbcConnnProperties = new Properties();
    jdbcConnnProperties.put("user", db.getUsername());
    jdbcConnnProperties.put("password", db.getPassword());

    if (VERIFY_EXPECTED) {
      verify(2);
      clear();
    }
  }

  private static void clear() {
    mockServer
        .clear(request().withMethod("POST").withPath("/aspects").withQueryStringParameter("action", "ingestProposal"));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    spark.stop();
    mockServer.stop();
  }

  private static void check(List<DatasetLineage> expected, List<DatasetLineage> actual) {
    assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      check(expected.get(i), actual.get(i));
    }
  }

  private static void check(DatasetLineage expected, DatasetLineage actual) {
    assertEquals(expected.getSink().toString(), actual.getSink().toString());
    assertEquals(dsToStrings(expected.getSources()), dsToStrings(actual.getSources()));
    assertTrue(actual.getCallSiteShort().contains("TestSparkJobsLineage"));
  }

  private static Set<String> dsToStrings(Set<SparkDataset> datasets) {
    return datasets.stream().map(x -> x.toString()).collect(Collectors.toSet());
  }

  private static DatasetLineage dsl(SparkDataset sink, SparkDataset... source) {
    return dsl(null, sink, source);
  }

  private static DatasetLineage dsl(String callSite, SparkDataset sink, SparkDataset... source) {
    DatasetLineage lineage = new DatasetLineage(callSite, "unknownPlan", sink);
    Arrays.asList(source).forEach(x -> lineage.addSource(x));
    return lineage;
  }

  private static HdfsPathDataset hdfsDs(String fileName) {
    return new HdfsPathDataset("file:" + abs(DATA_DIR + "/" + fileName), DATASET_PLATFORM_INSTANCE, DATASET_ENV);
  }

  private static JdbcDataset pgDs(String tbl) {
    return new JdbcDataset(db.getJdbcUrl(), tbl, DATASET_PLATFORM_INSTANCE, DATASET_ENV);
  }

  private static CatalogTableDataset catTblDs(String tbl) {
    return new CatalogTableDataset(tbl(tbl), DATASET_PLATFORM_INSTANCE, DATASET_ENV);
  }

  private static String tbl(String tbl) {
    return TEST_DB + "." + tbl;
  }

  private static String abs(String relPath) {
    return new File(relPath).getAbsolutePath();
  }

  @Before
  public void before() {
    acc.flushJobs();
  }

  @After
  public void after() {
    resetBaseExpectations();
  }

  @Test
  public void test1HdfsInOut() throws Exception {

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv");
    Dataset<Row> df2 = spark.read().option("header", "true").csv(DATA_DIR + "/in2.csv");
    df1.createOrReplaceTempView("v1");
    df2.createOrReplaceTempView("v2");

    Dataset<Row> df = spark
        .sql("select v1.c1 as a, v1.c2 as b, v2.c1 as c, v2.c2 as d from v1 join v2 on v1.id = v2.id");

    // InsertIntoHadoopFsRelationCommand
    df.write().mode(SaveMode.Overwrite).csv(DATA_DIR + "/out.csv");
    Thread.sleep(5000);
    check(dsl(hdfsDs("out.csv"), hdfsDs("in1.csv"), hdfsDs("in2.csv")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }

  @Test
  public void test5HdfsInJdbcOut() throws Exception {

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b");

    Dataset<Row> df2 = spark.read().option("header", "true").csv(DATA_DIR + "/in2.csv").withColumnRenamed("c1", "c")
        .withColumnRenamed("c2", "d");

    Dataset<Row> df = df1.join(df2, "id").drop("id");

    // SaveIntoDataSourceCommand
    // HadoopFsRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(db.getJdbcUrl(), "foo1", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo1"), hdfsDs("in1.csv"), hdfsDs("in2.csv")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }

  @Test
  public void test3HdfsJdbcInJdbcOut() throws Exception {

    Connection c = db.createConnection("");
    c.createStatement().execute("create table foo2 (a varchar(5), b int);");
    c.createStatement().execute("insert into foo2 values('a', 4);");
    c.close();

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b2");

    Dataset<Row> df2 = spark.read().jdbc(db.getJdbcUrl(), "foo2", jdbcConnnProperties);

    Dataset<Row> df = df1.join(df2, "a");

    // SaveIntoDataSourceCommand
    // JDBCRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(db.getJdbcUrl(), "foo3", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo3"), hdfsDs("in1.csv"), pgDs("foo2")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }

  @Test
  public void test2HdfsInHiveOut() throws Exception {

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b");

    Dataset<Row> df2 = spark.read().option("header", "true").csv(DATA_DIR + "/in2.csv").withColumnRenamed("c1", "c")
        .withColumnRenamed("c2", "d");

    Dataset<Row> df = df1.join(df2, "id").drop("id");

    df.write().mode(SaveMode.Overwrite).saveAsTable(tbl("foo4")); // CreateDataSourceTableAsSelectCommand
    df.write().mode(SaveMode.Append).saveAsTable(tbl("foo4")); // CreateDataSourceTableAsSelectCommand
    df.write().insertInto(tbl("foo4")); // InsertIntoHadoopFsRelationCommand

    Thread.sleep(5000);
    // TODO same data accessed as Hive Table or Path URI ??

    DatasetLineage exp = dsl(catTblDs("foo4"), hdfsDs("in1.csv"), hdfsDs("in2.csv"));
    check(Collections.nCopies(3, exp), acc.getLineages());
    if (VERIFY_EXPECTED) {
      verify(3 * N);
    }
  }

  @Test
  public void test4HiveInHiveOut() throws Exception {

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b");

    Dataset<Row> df2 = spark.read().option("header", "true").csv(DATA_DIR + "/in2.csv").withColumnRenamed("c1", "c")
        .withColumnRenamed("c2", "d");

    df1.createOrReplaceTempView("v1");
    df2.createOrReplaceTempView("v2");

    // CreateHiveTableAsSelectCommand
    spark.sql(
        "create table " + tbl("foo5") + " as " + "(select v1.a, v1.b, v2.c, v2.d from v1 join v2 on v1.id = v2.id)");

    check(dsl(catTblDs("foo5"), hdfsDs("in1.csv"), hdfsDs("in2.csv")), acc.getLineages().get(0));

    // CreateHiveTableAsSelectCommand
    spark.sql("create table " + tbl("hivetab") + " as " + "(select * from " + tbl("foo5") + ")");

    check(dsl(catTblDs("hivetab"), catTblDs("foo5")), acc.getLineages().get(1));

    // InsertIntoHiveTable
    spark.sql("insert into " + tbl("hivetab") + " (select * from " + tbl("foo5") + ")");
    check(dsl(catTblDs("hivetab"), catTblDs("foo5")), acc.getLineages().get(2));

    Dataset<Row> df = spark.sql("select * from " + tbl("foo5"));

    // InsertIntoHiveTable
    df.write().insertInto(tbl("hivetab"));
    Thread.sleep(5000);
    check(dsl(catTblDs("hivetab"), catTblDs("foo5")), acc.getLineages().get(3));
    if (VERIFY_EXPECTED) {
      verify(4 * N);
    }
  }

  @Test
  public void test6HdfsJdbcInJdbcOutTwoLevel() throws Exception {

    Connection c = db.createConnection("");
    c.createStatement().execute("create table foo6 (a varchar(5), b int);");
    c.createStatement().execute("insert into foo6 values('a', 4);");
    c.close();

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b2");

    Dataset<Row> df2 = spark.read().jdbc(db.getJdbcUrl(), "foo6", jdbcConnnProperties);

    Dataset<Row> df3 = spark.read().option("header", "true").csv(DATA_DIR + "/in2.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b3");

    Dataset<Row> df = df1.join(df2, "a").drop("id").join(df3, "a");

    // SaveIntoDataSourceCommand
    // JDBCRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(db.getJdbcUrl(), "foo7", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo7"), hdfsDs("in1.csv"), hdfsDs("in2.csv"), pgDs("foo6")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }

  @Test
  public void test7HdfsInPersistHdfsOut() throws Exception {

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in3.csv");

    Dataset<Row> df2 = spark.read().option("header", "true").csv(DATA_DIR + "/in4.csv").withColumnRenamed("c2", "d")
        .withColumnRenamed("c1", "c").withColumnRenamed("id", "id2");
    Dataset<Row> df = df1.join(df2, df1.col("id").equalTo(df2.col("id2")), "inner")
        .filter(df1.col("id").equalTo("id_filter")).persist(StorageLevel.MEMORY_ONLY());

    df.show();
    // InsertIntoHadoopFsRelationCommand
    df.write().mode(SaveMode.Overwrite).csv(DATA_DIR + "/out_persist.csv");
    Thread.sleep(5000);
    check(dsl(hdfsDs("out_persist.csv"), hdfsDs("in3.csv"), hdfsDs("in4.csv")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }

  @Test
  public void test8PersistHdfsJdbcInJdbcOut() throws Exception {

    Connection c = db.createConnection("");
    c.createStatement().execute("create table foo8 (a varchar(5), b int);");
    c.createStatement().execute("insert into foo8 values('a', 4);");
    c.close();

    Dataset<Row> df1 = spark.read().option("header", "true").csv(DATA_DIR + "/in1.csv").withColumnRenamed("c1", "a")
        .withColumnRenamed("c2", "b2");

    Dataset<Row> df2 = spark.read().jdbc(db.getJdbcUrl(), "foo8", jdbcConnnProperties).persist(StorageLevel.MEMORY_ONLY());

    Dataset<Row> df = df1.join(df2, "a");

    // SaveIntoDataSourceCommand
    // JDBCRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(db.getJdbcUrl(), "foo9", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo9"), hdfsDs("in1.csv"), pgDs("foo8")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }
  
  // This test cannot be executed individually. It depends upon previous tests to create tables in the database.
  @Test
  public void test9PersistJdbcInHdfsOut() throws Exception {

    Connection c = db.createConnection("");
    
    Dataset<Row> df1 = spark.read().jdbc(db.getJdbcUrl(), "foo9", jdbcConnnProperties);
    df1 = df1.withColumnRenamed("b", "b1");
    Dataset<Row> df2 = spark.read().jdbc(db.getJdbcUrl(), "foo8", jdbcConnnProperties).persist(StorageLevel.DISK_ONLY_2());

    Dataset<Row> df = df1.join(df2, "a");
    
    df.write().mode(SaveMode.Overwrite).csv(DATA_DIR + "/out_persist.csv");
    Thread.sleep(5000);
    check(dsl(hdfsDs("out_persist.csv"), pgDs("foo2"), pgDs("foo3")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N);
    }
  }
  
  private static class DatasetLineageAccumulator implements LineageConsumer {

    boolean closed = false;

    private final List<DatasetLineage> lineages = new ArrayList<>();

    public void flushJobs() {
      lineages.clear();
    }

    public List<DatasetLineage> getLineages() {
      return Collections.unmodifiableList(lineages);
    }

    @Override
    public void accept(LineageEvent e) {
      if (closed) {
        throw new RuntimeException("Called after close");
      }
      if (e instanceof SQLQueryExecStartEvent) {
        lineages.add(((SQLQueryExecStartEvent) e).getDatasetLineage());
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
    }
  }
}
