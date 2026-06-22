package datahub.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Builds a real local SparkSession with the listener + file emitter, runs the job, parses MCPs.
   */
  protected EmittedMetadata runJob(Map<String, String> extraConf, Consumer<SparkSession> job)
      throws Exception {
    Path mcpFile = Files.createTempFile("spark-mcps", ".json");
    SparkSession.Builder builder =
        SparkSession.builder()
            .appName("listener-smoke")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.extraListeners", "datahub.spark.DatahubSparkListener")
            .config("spark.datahub.emitter", "file")
            .config("spark.datahub.file.filename", mcpFile.toString());
    extraConf.forEach(builder::config);

    SparkSession spark = builder.getOrCreate();
    try {
      job.accept(spark);
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
    return EmittedMetadata.parse(new String(Files.readAllBytes(mcpFile)));
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
   * The {@code connections} entries keyed by the OpenLineage namespace for this Postgres source.
   */
  protected static Map<String, String> connectionConf(PostgreSQLContainer<?> pg, String instance) {
    String key =
        "postgres://" + pg.getHost() + ":" + pg.getMappedPort(PostgreSQLContainer.POSTGRESQL_PORT);
    Map<String, String> conf = new LinkedHashMap<>();
    conf.put(
        "spark.datahub.metadata.dataset.connections.\"" + key + "\".platformInstance", instance);
    conf.put("spark.datahub.metadata.dataset.connections.\"" + key + "\".env", "PROD");
    return conf;
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

  /** Parsed view of the file-emitter output (a JSON array of MCPs) with simple query helpers. */
  protected static final class EmittedMetadata {
    final String raw;
    final List<JsonNode> mcps;

    private EmittedMetadata(String raw, List<JsonNode> mcps) {
      this.raw = raw;
      this.mcps = mcps;
    }

    static EmittedMetadata parse(String raw) {
      List<JsonNode> mcps = new ArrayList<>();
      try {
        if (raw != null && !raw.trim().isEmpty()) {
          JsonNode arr = MAPPER.readTree(raw);
          if (arr.isArray()) {
            arr.forEach(mcps::add);
          }
        }
      } catch (Exception e) {
        // Fall back to raw-only assertions if the array isn't well-formed.
      }
      return new EmittedMetadata(raw, mcps);
    }

    boolean contains(String s) {
      return raw != null && raw.contains(s);
    }

    boolean hasAspect(String aspectName) {
      return mcps.stream()
          .anyMatch(
              m -> m.hasNonNull("aspectName") && aspectName.equals(m.get("aspectName").asText()));
    }
  }
}
