package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression guard for issue #19289. Runs against the real shaded {@code shadowJar_2_13} on a Spark
 * 4 / Scala 2.13 runtime (see the {@code sparkSmoke4RuntimeOnly} wiring in build.gradle), which is
 * the only place this defect is observable: the unit and {@code sparkRealSmokeTest} suites run on
 * Scala 2.12, where the descriptors match by construction.
 *
 * <p>javac bakes the declared return type of a call into its invokevirtual descriptor, and the JVM
 * resolves methods on the full descriptor — return type included. Spark's Seq-returning APIs have a
 * different descriptor per cross-build ({@code scala.collection.Seq} on 2.12, {@code
 * scala.collection.immutable.Seq} on 2.13), so project classes compiled against Scala 2.12 and
 * shipped inside {@code acryl-spark-lineage_2.13} throw {@link NoSuchMethodError} on contact. On
 * the RDD path that is fatal rather than merely lossy: it is an {@link Error}, so the {@code catch
 * (Exception)} guards in the listener do not stop it and Spark's {@code tryOrStopSparkContext}
 * shuts the application down.
 */
public class ShadedScalaBinaryVersionTest {

  private static final String RDD_EXTRACTOR =
      "io.acryl.shaded.io.openlineage.spark.agent.util.RddDatasetInfoExtractor";

  /**
   * Descriptor of a no-argument Spark API returning a Scala 2.12 {@code Seq}. Constant-pool entries
   * hold descriptors as plain UTF-8, so this is searchable in the raw class bytes. Its 2.13
   * counterpart is not usable as the inverse signal: {@code ScalaConversionUtils.asScalaSeqEmpty()}
   * returns {@code immutable.Seq} in <em>both</em> cross-builds.
   */
  private static final String SCALA_212_SEQ_DESCRIPTOR = "()Lscala/collection/Seq;";

  private static final List<String> OWN_JAR_PREFIXES =
      Arrays.asList(
          "io/acryl/shaded/io/openlineage/", "io/acryl/shaded/io/datahubproject/", "datahub/");

  /**
   * Drives the path that killed the SparkContext: {@code onJobStart →
   * RddExecutionContext.findInputs → RddDatasetInfoExtractor}, over a real {@code FileScanRDD}
   * (Parquet scan) and a real {@code UnionRDD}. Calling the extractor directly rather than through
   * a full job keeps the failure legible — a job would surface it as an unrelated "SparkContext has
   * been shut down".
   */
  @Test
  public void rddExtractionResolvesSparkScala213Descriptors(@TempDir Path tmp) throws Exception {
    SparkSession spark =
        SparkSession.builder()
            .appName("scala213-descriptors")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
    try {
      Path left = tmp.resolve("left");
      Path right = tmp.resolve("right");
      spark.range(4).write().mode(SaveMode.Overwrite).parquet(left.toString());
      spark.range(4).write().mode(SaveMode.Overwrite).parquet(right.toString());

      Dataset<Row> leftDf = spark.read().parquet(left.toString());
      Dataset<Row> rightDf = spark.read().parquet(right.toString());

      // executedPlan().execute() rather than toRdd(): the latter wraps everything in an
      // SQLExecutionRDD, which the extractor does not recurse through, so nothing would reach the
      // FileScanRDD branch. What the listener sees at job start is the unwrapped chain below —
      // MapPartitionsRDD -> [UnionRDD ->] FileScanRDD.
      //
      // FileScanRDD.filePartitions(): the site that threw NoSuchMethodError and stopped the
      // SparkContext.
      List<String> fromScan = extractDatasetNames(leftDf.queryExecution().executedPlan().execute());
      assertTrue(
          fromScan.stream().anyMatch(n -> n.endsWith("left")),
          "Parquet scan produced no input dataset on Scala 2.13; got " + fromScan);

      // UnionRDD.rdds(): the same descriptor hazard one level up, and it recurses back into the
      // FileScanRDD branch for each side.
      List<String> fromUnion =
          extractDatasetNames(leftDf.union(rightDf).queryExecution().executedPlan().execute());
      assertTrue(
          fromUnion.stream().anyMatch(n -> n.endsWith("left"))
              && fromUnion.stream().anyMatch(n -> n.endsWith("right")),
          "Union did not yield both parquet inputs on Scala 2.13; got " + fromUnion);
    } finally {
      spark.stop();
      SparkSession.clearActiveSession();
      SparkSession.clearDefaultSession();
    }
  }

  /**
   * Covers the remaining call sites the extractor test cannot reach without a Delta or JDBC
   * dependency — {@code LogicalPlan.output()} in {@code SaveIntoDataSourceCommandVisitor} and
   * {@code RedshiftSaveIntoDataSourceCommandBuilder}, and {@code TreeNode.children()} in the two
   * Delta MERGE builders — by asserting the invariant those sites depend on directly on the
   * deployed artifact. {@code scripts/check_jar.sh} enforces the same thing at build time; this is
   * the runtime half, on the classpath where a mismatch actually fails.
   */
  @Test
  public void shadedJarCarriesNoScala212SeqDescriptors() throws Exception {
    File jarFile = shadedAgentJar();
    List<String> offenders = new ArrayList<>();
    try (JarFile jar = new JarFile(jarFile)) {
      Enumeration<JarEntry> entries = jar.entries();
      while (entries.hasMoreElements()) {
        JarEntry entry = entries.nextElement();
        String name = entry.getName();
        if (!name.endsWith(".class") || OWN_JAR_PREFIXES.stream().noneMatch(name::startsWith)) {
          continue;
        }
        try (InputStream in = jar.getInputStream(entry)) {
          // ISO-8859-1 keeps the bytes one-to-one; descriptors are ASCII either way.
          if (new String(readAll(in), StandardCharsets.ISO_8859_1)
              .contains(SCALA_212_SEQ_DESCRIPTOR)) {
            offenders.add(name);
          }
        }
      }
    }
    assertTrue(
        offenders.isEmpty(),
        "Scala 2.12 method descriptors ("
            + SCALA_212_SEQ_DESCRIPTOR
            + ") in the Scala 2.13 agent jar "
            + jarFile.getName()
            + " — these calls resolve to nothing on Scala 2.13 (issue #19289): "
            + offenders);
  }

  @SuppressWarnings("unchecked")
  private static List<String> extractDatasetNames(RDD<?> rdd) throws Exception {
    Method findDatasetIdentifiers =
        Class.forName(RDD_EXTRACTOR).getMethod("findDatasetIdentifiers", RDD.class);
    List<?> identifiers;
    try {
      // The extractor returns a lazy Stream, so the descriptors are only resolved once it is
      // consumed — collecting here is what makes a NoSuchMethodError surface.
      identifiers =
          ((Stream<Object>) findDatasetIdentifiers.invoke(null, rdd)).collect(Collectors.toList());
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof NoSuchMethodError) {
        fail(
            "The shaded Scala 2.13 agent calls a Spark API through a Scala 2.12 descriptor"
                + " (issue #19289). In a real job this Error escapes the listener's catch blocks"
                + " and Spark's tryOrStopSparkContext shuts the application down: "
                + e.getCause().getMessage());
      }
      throw e;
    }
    assertFalse(identifiers.isEmpty(), "extractor returned no dataset identifiers at all");
    List<String> names = new ArrayList<>();
    for (Object identifier : identifiers) {
      names.add((String) identifier.getClass().getMethod("getName").invoke(identifier));
    }
    return Collections.unmodifiableList(names);
  }

  /** The jar under test is wherever the relocated agent classes were loaded from. */
  private static File shadedAgentJar() throws Exception {
    URL location = Class.forName(RDD_EXTRACTOR).getProtectionDomain().getCodeSource().getLocation();
    File file = new File(location.toURI());
    if (!file.isFile()) {
      fail(
          "Expected "
              + RDD_EXTRACTOR
              + " to come from the shaded agent jar, but it was loaded from "
              + file
              + ". This test only means anything against the packaged artifact.");
    }
    return file;
  }

  private static byte[] readAll(InputStream in) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int read;
    while ((read = in.read(buffer)) != -1) {
      out.write(buffer, 0, read);
    }
    return out.toByteArray();
  }
}
