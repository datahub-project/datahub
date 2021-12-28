package com.linkedin.datahub.lineage;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.testcontainers.containers.PostgreSQLContainer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.MappingBuilder;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.Admin;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.extension.Parameters;
import com.github.tomakehurst.wiremock.extension.PostServeAction;
import com.github.tomakehurst.wiremock.matching.MatchResult;
import com.github.tomakehurst.wiremock.matching.StringValuePattern;
import com.github.tomakehurst.wiremock.stubbing.ServeEvent;
import com.linkedin.datahub.lineage.spark.interceptor.LineageUtils;
import com.linkedin.datahub.lineage.spark.model.DatasetLineage;
import com.linkedin.datahub.lineage.spark.model.LineageConsumer;
import com.linkedin.datahub.lineage.spark.model.LineageEvent;
import com.linkedin.datahub.lineage.spark.model.SQLQueryExecStartEvent;
import com.linkedin.datahub.lineage.spark.model.dataset.CatalogTableDataset;
import com.linkedin.datahub.lineage.spark.model.dataset.HdfsPathDataset;
import com.linkedin.datahub.lineage.spark.model.dataset.JdbcDataset;
import com.linkedin.datahub.lineage.spark.model.dataset.SparkDataset;



public class TestSparkJobsLineage {
  private static final boolean MOCK_GMS = Boolean.valueOf("true"); // if false, MCPs get written to real GMS server (see GMS_PORT)
  private static final boolean VERIFY_EXPECTED = MOCK_GMS && Boolean.valueOf("true"); // if false, "expected" JSONs are overwritten.

  private static final String APP_NAME = "sparkTestApp";

  private static final String RESOURCE_DIR = "src/test/resources";
  private static final String DATA_DIR = RESOURCE_DIR + "/data";
  private static final String WAREHOUSE_LOC = DATA_DIR + "/hive/warehouse";
  private static final String TEST_DB = "sparktestdb";

  private static final String MASTER = "local";

  private static final int N = 3; // num of GMS requests per spark job

  private static final int GMS_PORT = MOCK_GMS ? 8089 : 8080;

  private static final String EXPECTED_JSON_ROOT = "src/test/resources/expected/";

  private static SparkSession spark;
  private static Properties jdbcConnnProperties;
  private static DatasetLineageAccumulator acc;

  @SuppressWarnings("rawtypes")
  private static final class McpContentPattern extends StringValuePattern {

    public McpContentPattern(String expectedValue) {
      super(expectedValue);
    }

    // dataflow case, we do not match against expected string
    public McpContentPattern() {
      super("");
    }

    @Override
    public MatchResult match(String actual) {
      if (actual.contains("dataJobInputOutput")) {
        return expectedValue.contains(relPaths(actual)) ? MatchResult.exactMatch() : MatchResult.noMatch();
      }
      try {
        HashMap body = new ObjectMapper().readValue(actual, HashMap.class);
        HashMap proposal = (HashMap) body.get("proposal");
        String aspectName = (String) proposal.get("aspectName");
        if (aspectName.equals("dataFlowInfo")) {
          return checkFlowInfo(proposal);
        }
        if (actual.contains("dataJobInfo")) {
          return checkJobInfo(proposal);
        }
        return MatchResult.noMatch();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    private MatchResult checkJobInfo(HashMap proposal) {
      // TODO check custom props etc.
      return MatchResult.exactMatch();
    }

    private MatchResult checkFlowInfo(HashMap proposal) {
      // TODO check custom props etc.
      return MatchResult.exactMatch();
    }

  }

  private static String relPaths(String s) {
    return s.replaceAll("file:[0-9|a-z|A-Z|\\-|\\/|_|\\.]*" + RESOURCE_DIR, "file:/" + RESOURCE_DIR);
  }

  private static final class RequestFileWriter extends PostServeAction {

    @Override
    public String getName() {
      return "writeReqJson";
    }

    @SuppressWarnings({ "rawtypes", "unused" })
    public void doAction(ServeEvent serveEvent, Admin admin, Parameters parameters) {
      String currentTestRes = parameters.getString("filename");
      if (currentTestRes == null || VERIFY_EXPECTED) {
        return;
      }

      Path p = Paths.get(EXPECTED_JSON_ROOT, currentTestRes);
      System.out.println("Writing json to file " + p);
      String json = serveEvent.getRequest().getBodyAsString();
      try {
        HashMap body = new ObjectMapper().readValue(json, HashMap.class);
        HashMap proposal = (HashMap) body.get("proposal");
        String aspectName = (String) proposal.get("aspectName");

        // this effectively checks URNs and lineages; other jobInfo/flowInfo are checked
        // in McpContentPattern
        if (aspectName.equals("dataJobInputOutput")) {
          json = relPaths(json);
          Files.write(p, Collections.singletonList(json), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        }
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static class DatasetLineageAccumulator implements LineageConsumer {

    private final List<DatasetLineage> lineages = new ArrayList<>();

    public void flushJobs() {
      lineages.clear();
    }

    public List<DatasetLineage> getLineages() {
      return Collections.unmodifiableList(lineages);
    }

    @Override
    public void accept(LineageEvent e) {
      if (e instanceof SQLQueryExecStartEvent) {
        lineages.add(((SQLQueryExecStartEvent) e).getDatasetLineage());
      }
    }
  }

  @Rule
  public TestRule watcher = new TestWatcher() {
    protected void starting(Description description) {
      if (!MOCK_GMS) {
        return;
      }
      String currentTestRes = description.getMethodName() + ".json";

      MappingBuilder mapping = baseMapping();

      try {
        if (VERIFY_EXPECTED) {
          // setup the request body that we expect
          List<String> expected = Files.readAllLines(Paths.get(EXPECTED_JSON_ROOT, currentTestRes));
          mapping.withRequestBody(new McpContentPattern(expected.toString()));
        } else {
          // overwrite "expected" json file with the MCP request bodies
          mapping.withPostServeAction("writeReqJson", Parameters.one("filename", currentTestRes));
          Files.deleteIfExists(Paths.get(EXPECTED_JSON_ROOT, currentTestRes));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
      wireMock.stubFor(mapping);
    }

    @Override
    protected void finished(Description description) {
      if (!VERIFY_EXPECTED) {
        return;
      }
      assertTrue(WireMock.findUnmatchedRequests().isEmpty());
      wireMock.resetRequests();
      wireMock.resetMappings();
      super.finished(description);
    }
  };

  public static WireMockServer wireMock = new WireMockServer(WireMockConfiguration
      .options()
      .port(8089)
      .extensions(new RequestFileWriter()));

  @ClassRule
  public static PostgreSQLContainer<?> db = new PostgreSQLContainer<>("postgres:9.6.12")
      .withDatabaseName("sparktestdb");

  private static MappingBuilder baseMapping() {
    return post("/aspects?action=ingestProposal")
        .willReturn(ok()
            .withBody("<response>SUCCESS</response>"));
  }

  @BeforeClass
  public static void setup() {
    
    acc = new DatasetLineageAccumulator();
    LineageUtils.registerConsumer("accumulator", acc);

    wireMock.start();
    WireMock.configureFor("localhost", 8089);
    MappingBuilder mapping = baseMapping();
    if (VERIFY_EXPECTED) {
      mapping.withRequestBody(new McpContentPattern());
    }
    wireMock.stubFor(mapping);

    spark = SparkSession
        .builder()
        .appName(APP_NAME)
        .config("spark.master", MASTER)
        .config("spark.extraListeners",
            "com.linkedin.datahub.lineage.spark.interceptor.DatahubLineageEmitter")
        .config("spark.datahub.lineage.consumerTypes", "accumulator, mcpEmitter")
        .config("spark.datahub.lineage.mcpEmitter.gmsUrl", "http://localhost:" + GMS_PORT)
        .config("spark.sql.warehouse.dir", new File(WAREHOUSE_LOC).getAbsolutePath())
        .enableHiveSupport()
        .getOrCreate();

    spark.sql("drop database if exists " + TEST_DB + " cascade");
    spark.sql("create database " + TEST_DB);
    jdbcConnnProperties = new Properties();
    jdbcConnnProperties.put("user", db.getUsername());
    jdbcConnnProperties.put("password", db.getPassword());

    if (VERIFY_EXPECTED) {
      verify(1, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
      assertTrue(WireMock.findUnmatchedRequests().isEmpty());
    }
    wireMock.resetRequests();
    wireMock.resetMappings();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    wireMock.resetRequests();
    wireMock.resetMappings();
    MappingBuilder mapping = baseMapping();
    if (VERIFY_EXPECTED) {
      mapping.withRequestBody(new McpContentPattern());
    }
    wireMock.stubFor(mapping);
    spark.stop();
    if (VERIFY_EXPECTED) {
      verify(1, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
      assertTrue(WireMock.findUnmatchedRequests().isEmpty());
    }
    wireMock.stop();
  }

  @Before
  public void before() {
    acc.flushJobs();
  }

  @Test
  public void testHdfsInOut() throws Exception {
    
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
      verify(1 * N, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
    }
  }

  @Test
  public void testHdfsInJdbcOut() throws Exception {
    Dataset<Row> df1 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in1.csv")
        .withColumnRenamed("c1", "a").withColumnRenamed("c2", "b");

    Dataset<Row> df2 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in2.csv")
        .withColumnRenamed("c1", "c").withColumnRenamed("c2", "d");

    Dataset<Row> df = df1.join(df2, "id").drop("id");

    // SaveIntoDataSourceCommand
    // HadoopFsRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(
        db.getJdbcUrl(),
        "foo1", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo1"), hdfsDs("in1.csv"), hdfsDs("in2.csv")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
    }
  }

  @Test
  public void testHdfsJdbcInJdbcOut() throws Exception {
    Connection c = db.createConnection("");
    c.createStatement().execute("create table foo2 (a varchar(5), b int);");
    c.createStatement().execute("insert into foo2 values('a', 4);");
    c.close();

    Dataset<Row> df1 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in1.csv")
        .withColumnRenamed("c1", "a").withColumnRenamed("c2", "b2");

    Dataset<Row> df2 = spark.read()
        .jdbc(db.getJdbcUrl(), "foo2", jdbcConnnProperties);

    Dataset<Row> df = df1.join(df2, "a");

    // SaveIntoDataSourceCommand
    // JDBCRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(
        db.getJdbcUrl(),
        "foo3", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo3"), hdfsDs("in1.csv"), pgDs("foo2")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
    }
  }

  @Test
  public void testHdfsInHiveOut() throws Exception {
    Dataset<Row> df1 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in1.csv")
        .withColumnRenamed("c1", "a").withColumnRenamed("c2", "b");

    Dataset<Row> df2 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in2.csv")
        .withColumnRenamed("c1", "c").withColumnRenamed("c2", "d");

    Dataset<Row> df = df1.join(df2, "id").drop("id");

    df.write().mode(SaveMode.Overwrite).saveAsTable(tbl("foo4")); // CreateDataSourceTableAsSelectCommand
    df.write().mode(SaveMode.Append).saveAsTable(tbl("foo4")); // CreateDataSourceTableAsSelectCommand
    df.write().insertInto(tbl("foo4")); // InsertIntoHadoopFsRelationCommand

    Thread.sleep(5000);
    // TODO same data accessed as Hive Table or Path URI ??

    DatasetLineage exp = dsl(catTblDs("foo4"), hdfsDs("in1.csv"), hdfsDs("in2.csv"));
    check(Collections.nCopies(3, exp), acc.getLineages());
    if (VERIFY_EXPECTED) {
      verify(3 * N, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
    }
  }

  @Test
  public void testHiveInHiveOut() throws Exception {
    Dataset<Row> df1 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in1.csv")
        .withColumnRenamed("c1", "a").withColumnRenamed("c2", "b");

    Dataset<Row> df2 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in2.csv")
        .withColumnRenamed("c1", "c").withColumnRenamed("c2", "d");

    df1.createOrReplaceTempView("v1");
    df2.createOrReplaceTempView("v2");

    // CreateHiveTableAsSelectCommand
    spark.sql("create table " + tbl("foo5") + " as "
        + "(select v1.a, v1.b, v2.c, v2.d from v1 join v2 on v1.id = v2.id)");

    check(dsl(catTblDs("foo5"), hdfsDs("in1.csv"), hdfsDs("in2.csv")), acc.getLineages().get(0));

    // CreateHiveTableAsSelectCommand
    spark.sql("create table " + tbl("hivetab") + " as "
        + "(select * from " + tbl("foo5") + ")");

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
      verify(4 * N, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
    }
  }

  @Test
  public void testHdfsJdbcInJdbcOutTwoLevel() throws Exception {
    Connection c = db.createConnection("");
    c.createStatement().execute("create table foo6 (a varchar(5), b int);");
    c.createStatement().execute("insert into foo6 values('a', 4);");
    c.close();

    Dataset<Row> df1 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in1.csv")
        .withColumnRenamed("c1", "a").withColumnRenamed("c2", "b2");

    Dataset<Row> df2 = spark.read()
        .jdbc(db.getJdbcUrl(), "foo6", jdbcConnnProperties);

    Dataset<Row> df3 = spark.read()
        .option("header", "true").csv(DATA_DIR + "/in2.csv")
        .withColumnRenamed("c1", "a").withColumnRenamed("c2", "b3");

    Dataset<Row> df = df1.join(df2, "a").drop("id").join(df3, "a");

    // SaveIntoDataSourceCommand
    // JDBCRelation input
    df.write().mode(SaveMode.Overwrite).jdbc(
        db.getJdbcUrl(),
        "foo7", jdbcConnnProperties);
    Thread.sleep(5000);
    check(dsl(pgDs("foo7"), hdfsDs("in1.csv"), hdfsDs("in2.csv"), pgDs("foo6")), acc.getLineages().get(0));
    if (VERIFY_EXPECTED) {
      verify(1 * N, postRequestedFor(urlEqualTo("/aspects?action=ingestProposal")));
    }
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
    return new HdfsPathDataset("file:" + abs(DATA_DIR + "/" + fileName));
  }

  private static JdbcDataset pgDs(String tbl) {
    return new JdbcDataset(db.getJdbcUrl(), tbl);
  }

  private static CatalogTableDataset catTblDs(String tbl) {
    return new CatalogTableDataset(tbl(tbl));
  }

  private static String tbl(String tbl) {
    return TEST_DB + "." + tbl;
  }

  private static String abs(String relPath) {
    return new File(relPath).getAbsolutePath();
  }
}