package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Glue connection-instance mapping, end-to-end in-JVM. A real Spark job reads an Iceberg table via
 * {@code GlueCatalog} pointed at a mocked Glue/S3 (moto), so OpenLineage emits the table with an
 * {@code arn:aws:glue:<region>:<account>} symlink; the listener resolves that catalog ARN to the
 * mapped {@code platform_instance}.
 *
 * <p>The crucial detail: OpenLineage builds the Glue ARN from the region + account, and falls back
 * to a live STS {@code GetCallerIdentity} call when the account isn't configured. That STS fallback
 * doesn't resolve for an in-process {@code local[*]} SparkSession, so we set {@code
 * spark.glue.accountId} explicitly — then the symlink is generated without any STS round-trip and
 * the scenario runs in-JVM (no spark-submit harness needed).
 */
@Tag("integration")
public class SparkGlueConnectionInstanceSmokeTest extends SparkSmokeTestBase {

  // moto's fixed default account; us-east-1 is the region we point the Glue client at.
  private static final String ACCOUNT = "123456789012";
  private static final String GLUE_ARN = "arn:aws:glue:us-east-1:" + ACCOUNT;
  private static final int MOTO_PORT = 5000;

  private static GenericContainer<?> moto;
  private static String endpoint;

  @BeforeAll
  static void start() throws Exception {
    moto =
        new GenericContainer<>(DockerImageName.parse("motoserver/moto:5.1.0"))
            .withExposedPorts(MOTO_PORT)
            .waitingFor(Wait.forListeningPort());
    moto.start();
    endpoint = "http://" + moto.getHost() + ":" + moto.getMappedPort(MOTO_PORT);

    // S3FileIO does not create the warehouse bucket; moto accepts an unauthenticated path-style
    // PUT.
    HttpClient.newHttpClient()
        .send(
            HttpRequest.newBuilder(URI.create(endpoint + "/warehouse"))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build(),
            HttpResponse.BodyHandlers.discarding());

    // The Spark job runs in this JVM (local mode), so the in-process AWS SDK clients pick these up.
    System.setProperty("aws.accessKeyId", "test");
    System.setProperty("aws.secretAccessKey", "test");
    System.setProperty("aws.region", "us-east-1");
    System.setProperty("aws.endpointUrl", endpoint);
    System.setProperty("aws.endpointUrlS3", endpoint);
    System.setProperty("aws.endpointUrlGlue", endpoint);
    // AWS SDK >= 2.30 enables aws-chunked request encoding that moto stores literally (corrupting
    // Iceberg's metadata.json); restrict checksums to when-required so the mock receives plain
    // bodies.
    System.setProperty("aws.requestChecksumCalculation", "when_required");
    System.setProperty("aws.responseChecksumValidation", "when_required");
  }

  @AfterAll
  static void stop() {
    if (moto != null) {
      moto.stop();
    }
  }

  @Test
  public void stampsConnectionInstanceOnGlueUpstream(@TempDir Path tmp) throws Exception {
    Map<String, String> catalog = new LinkedHashMap<>();
    catalog.put(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
    catalog.put("spark.sql.catalog.glue_cat", "org.apache.iceberg.spark.SparkCatalog");
    catalog.put(
        "spark.sql.catalog.glue_cat.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
    catalog.put("spark.sql.catalog.glue_cat.warehouse", "s3://warehouse/");
    catalog.put("spark.sql.catalog.glue_cat.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    catalog.put("spark.sql.catalog.glue_cat.s3.endpoint", endpoint);
    catalog.put("spark.sql.catalog.glue_cat.s3.path-style-access", "true");
    catalog.put("spark.sql.catalog.glue_cat.glue.endpoint", endpoint);
    catalog.put("spark.sql.catalog.glue_cat.client.region", "us-east-1");
    // Give OpenLineage the Glue account id directly so it builds the arn:aws:glue symlink without
    // the live STS GetCallerIdentity fallback (which does not resolve for an in-process Spark).
    catalog.put("spark.glue.accountId", ACCOUNT);

    EmittedMetadata md =
        runJob(
            listener()
                .emitToFile(tmp.resolve("mcps.json"))
                .connection(GLUE_ARN, "domain_a", "PROD"),
            catalog,
            spark -> {
              spark.sql("CREATE DATABASE IF NOT EXISTS glue_cat.my_glue_database");
              spark.sql(
                  "CREATE TABLE IF NOT EXISTS glue_cat.my_glue_database.my_glue_table "
                      + "(id INT, amount INT) USING iceberg");
              spark.sql("INSERT INTO glue_cat.my_glue_database.my_glue_table VALUES (1, 100)");
              spark
                  .table("glue_cat.my_glue_database.my_glue_table")
                  .write()
                  .mode(SaveMode.Overwrite)
                  .csv(tmp.resolve("out").toString());
            });

    assertTrue(
        md.datasetUrnsOnPlatform("glue").stream()
            .anyMatch(u -> u.contains("glue,domain_a.my_glue_database.my_glue_table")),
        "glue upstream URN should carry the per-connection instance 'domain_a':\n" + md.raw);
  }
}
