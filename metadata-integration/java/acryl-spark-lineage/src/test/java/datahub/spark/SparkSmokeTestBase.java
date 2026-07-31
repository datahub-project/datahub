package datahub.spark;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Shared harness for the real-Spark listener smoke tests.
 *
 * <p>Each concrete subclass holds exactly ONE Spark test. This is deliberate: Spark allows only one
 * active SparkContext per JVM, and the DataHub/OpenLineage listener does not cleanly re-attach
 * across a stop/restart in the same JVM — so a single class with multiple Spark jobs silently emits
 * nothing for the later jobs. With {@code forkEvery = 1} (see build.gradle), one class == one JVM
 * == one SparkContext, which keeps each test isolated and reliable.
 *
 * <p>{@link #runJob} starts a real local SparkSession with the listener + file emitter, runs the
 * job, stops the session (flushing the emitter), and returns the parsed MCPs.
 */
abstract class SparkSmokeTestBase {

  /**
   * Builds a real local SparkSession with the listener, runs the job, parses MCPs from the file the
   * test declared via {@link ListenerConf#emitToFile}.
   *
   * <p>The effective DataHub listener configuration is printed before the job runs (see {@link
   * ListenerConf#describe}), so the test output shows exactly which knobs were on/off and how they
   * were set. Spark-harness settings (master, listener attachment) are added here; everything under
   * {@code spark.datahub.*} comes from the {@link ListenerConf} the test passes.
   */
  protected EmittedMetadata runJob(ListenerConf conf, Consumer<SparkSession> job) throws Exception {
    return runJob(conf, Collections.emptyMap(), job);
  }

  /**
   * Same as {@link #runJob(ListenerConf, Consumer)} but also applies {@code sparkInfraConf} —
   * non-DataHub Spark settings a test needs at session-build time (e.g. an Iceberg catalog
   * definition). Logged separately so both the listener config and the infra config are visible.
   */
  protected EmittedMetadata runJob(
      ListenerConf conf, Map<String, String> sparkInfraConf, Consumer<SparkSession> job)
      throws Exception {
    if (conf.emitFile() == null) {
      throw new IllegalStateException(
          "Test must declare an emitter, e.g. listener().emitToFile(tmp.resolve(\"mcps.json\"))");
    }
    System.out.println("\n" + conf.describe());
    if (!sparkInfraConf.isEmpty()) {
      StringBuilder sb = new StringBuilder("--- spark infra config (catalog, etc.) ---\n");
      sparkInfraConf.forEach(
          (k, v) -> sb.append("  ").append(k).append(" = ").append(v).append('\n'));
      System.out.println(sb);
    }

    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("listener-smoke")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            // Bind to loopback so stray hosts on the LAN can't connect to Spark's internal ports
            // (an external connection to the 0.0.0.0-bound block manager surfaces as "Too large
            // frame" and aborts SparkContext init).
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.extraListeners", "datahub.spark.DatahubSparkListener");
    sparkInfraConf.forEach(builder::config);
    conf.toSparkConf().forEach(builder::config);

    SparkSession spark = builder.getOrCreate();
    try {
      job.accept(spark);
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
    return EmittedMetadata.parse(new String(Files.readAllBytes(conf.emitFile())));
  }

  protected static void jdbcReadToCsv(
      SparkSession spark, PostgreSQLContainer<?> pg, String table, Path out) {
    spark
        .read()
        .jdbc(pg.getJdbcUrl(), table, jdbcProps(pg))
        .write()
        .mode(SaveMode.Overwrite)
        .csv(out.toString());
  }

  protected static Properties jdbcProps(PostgreSQLContainer<?> pg) {
    Properties p = new Properties();
    p.setProperty("user", pg.getUsername());
    p.setProperty("password", pg.getPassword());
    p.setProperty("driver", "org.postgresql.Driver");
    return p;
  }

  /**
   * The OpenLineage namespace DataHub derives for this Postgres source ({@code
   * postgres://host:port}) — use it as the key in {@link ListenerConf#connection}.
   */
  protected static String pgNamespace(PostgreSQLContainer<?> pg) {
    return "postgres://"
        + pg.getHost()
        + ":"
        + pg.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
  }

  protected static void seed(PostgreSQLContainer<?> pg, String... statements) throws Exception {
    try (Connection conn =
            DriverManager.getConnection(pg.getJdbcUrl(), pg.getUsername(), pg.getPassword());
        Statement st = conn.createStatement()) {
      for (String s : statements) {
        st.execute(s);
      }
    }
  }

  protected static Path writeCsv(Path file, String content) throws Exception {
    Files.write(file, content.getBytes());
    return file;
  }
}
