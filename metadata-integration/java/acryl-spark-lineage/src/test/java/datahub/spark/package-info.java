/**
 * Real-Spark feature regression suite for the DataHub Spark listener.
 *
 * <p>Each {@code *SmokeTest} class starts a real local {@link org.apache.spark.sql.SparkSession}
 * with the listener attached, runs one Spark job, and asserts on the metadata the listener emits to
 * a file. They are tagged {@code @Tag("integration")} and run via the {@code sparkRealSmokeTest}
 * Gradle task (wired into the {@code spark-smoke-test} CI workflow), not the fast unit run.
 *
 * <h2>Why one Spark job per class</h2>
 *
 * Spark allows only one active SparkContext per JVM, and the listener does not cleanly re-attach
 * across a stop/restart in the same JVM. With {@code forkEvery = 1} (see {@code build.gradle}), one
 * class == one JVM == one SparkContext. So each feature gets its own class with a single
 * {@code @Test}; do not add a second Spark job to a class.
 *
 * <h2>Building blocks</h2>
 *
 * <ul>
 *   <li>{@link datahub.spark.ListenerConf} — a typed, fluent builder for the {@code
 *       spark.datahub.*} configuration (with a {@code .set(key, value)} escape hatch for knobs
 *       without a typed method). {@code runJob} logs its {@code describe()} so the test output
 *       shows exactly which knobs were on/off and how they were set.
 *   <li>{@link datahub.spark.SparkSmokeTestBase} — {@code runJob(listener, job)} starts Spark, runs
 *       the job, and returns the emitted metadata. Plus JDBC/seed/CSV helpers.
 *   <li>{@link datahub.spark.EmittedMetadata} — query helpers over the emitted MCPs: {@code
 *       inputDatasetUrns()}, {@code outputDatasetUrns()}, {@code datasetUrnsOnPlatform(p)}, {@code
 *       aspect(name)}, {@code hasAspect(name)}, {@code hasEntity(type)}. Prefer these over raw
 *       substring matching.
 * </ul>
 *
 * <h2>Adding a feature test</h2>
 *
 * <pre>{@code
 * @Tag("integration")
 * public class SparkMyFeatureSmokeTest extends SparkSmokeTestBase {
 *   @Test
 *   public void emitsMyFeature(@TempDir Path tmp) throws Exception {
 *     Path in = writeCsv(tmp.resolve("in.csv"), "id,c\n1,x\n");
 *     EmittedMetadata md =
 *         runJob(
 *             listener().emitToFile(tmp.resolve("mcps.json")).myFeature(...),
 *             spark ->
 *                 spark.read().option("header", "true").csv(in.toString())
 *                     .write().mode(SaveMode.Overwrite).csv(tmp.resolve("out").toString()));
 *     assertTrue(md.datasetUrnsOnPlatform("file")... , md.raw);
 *   }
 * }
 * }</pre>
 *
 * <p>If {@code ListenerConf} lacks a typed method for the knob, add one (centralizing the key) or
 * use {@code .set(...)}.
 *
 * <h2>What runs where</h2>
 *
 * File-only feature tests (env, tags, domains, pipeline instance, lowerCaseUrns, materialize,
 * column-level/path lineage) run Docker-free. Tests that need a source container (the JDBC
 * connection-instance tests) use Testcontainers and require Docker.
 *
 * <p>Two features can't be exercised here and live elsewhere: Glue catalog symlinks only appear
 * under a real {@code spark-submit} run (see the {@code spark-smoke-test/} docker harness); and
 * {@code file_partition_regexp} / {@code path_spec_list} only apply to scheme-based namespaces
 * ({@code s3://}, {@code gs://}, {@code hdfs://}) that route through {@code HdfsPathDataset} — a
 * local {@code file} dataset (namespace {@code file}, no scheme) bypasses that path. They are
 * unit-tested directly in {@code HdfsPathDatasetTest} and belong against object storage in the
 * docker harness for end-to-end coverage.
 */
package datahub.spark;
