package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for issue #19005. Runs against the real shaded {@code shadowJar_2_13} (see the
 * {@code sparkSmoke4RuntimeOnly} wiring in build.gradle), so it exercises the exact artifact users
 * deploy — the unit and {@code sparkRealSmokeTest} suites run against the unshaded classpath and
 * therefore cannot see a relocation bug at all.
 *
 * <p>Shadow rewrites <em>string constants</em> in the constant pool, not just bytecode symbols.
 * When our code identifies a class by name — {@code Class.forName}, {@code loadClass}, or comparing
 * {@code getCanonicalName()} — and the runtime supplies that class from the <em>host</em> classpath
 * (Spark, its Kafka connector, spark-redshift), a rewritten literal can never match what we are
 * handed. The visitor then silently stops matching and the lineage is dropped with no error, which
 * is why this needs a test rather than trust.
 */
public class ShadedReflectionClassNameTest {

  private static final String SHADED_PREFIX = "io.acryl.shaded.";

  /**
   * {@code TopicPartitionProxy} validates its argument by comparing {@code getCanonicalName()}
   * against a {@code static final String}. Relocating {@code org.apache.kafka} rewrote that
   * constant to {@code io.acryl.shaded.org.apache.kafka.common.TopicPartition}, while the objects
   * in {@code KafkaSourceOffset.partitionToOffsets} come from Spark's own {@code
   * spark-sql-kafka-0-10} at the canonical name — so every partition of every micro-batch threw,
   * {@code PlanUtils.safeApply} swallowed it at INFO, and Kafka streaming reads produced no input
   * datasets at all.
   */
  @Test
  public void kafkaTopicPartitionProxyAcceptsHostSuppliedTopicPartition() throws Exception {
    // A genuine unshaded TopicPartition, exactly as Spark's Kafka connector would supply it.
    TopicPartition hostSupplied = new TopicPartition("topic-a", 0);

    Class<?> proxyClass =
        Class.forName(
            "io.acryl.shaded.io.openlineage.spark.agent.lifecycle.plan.TopicPartitionProxy");
    Constructor<?> ctor = proxyClass.getConstructor(Object.class);

    Object proxy;
    try {
      proxy = ctor.newInstance(hostSupplied);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof IllegalArgumentException) {
        fail(
            "Shaded TopicPartitionProxy rejected a host-supplied "
                + hostSupplied.getClass().getCanonicalName()
                + " (issue #19005) — its expected-class-name constant was rewritten by the"
                + " org.apache.kafka relocation, so Kafka streaming inputs are always empty: "
                + e.getCause().getMessage());
      }
      throw e;
    }

    Object topic = proxyClass.getMethod("topic").invoke(proxy);
    assertEquals(
        Optional.of("topic-a"), topic, "proxy should read the topic off the host TopicPartition");
    // Only topic() feeds the Kafka input dataset (KafkaMicroBatchStreamStrategy calls nothing
    // else),
    // but partition() is the proxy's other reflective accessor, so cover it too.
    Object partition = proxyClass.getMethod("partition").invoke(proxy);
    assertEquals(
        Optional.of(0), partition, "proxy should read the partition off the host TopicPartition");
  }

  /**
   * The Redshift vendor resolves {@code RedshiftRelation} with {@code
   * loadClass(REDSHIFT_CLASS_NAME)} and swallows the failure, so a rewritten constant makes {@code
   * isDefinedAt} return false forever with no log line at all. spark-redshift-community is {@code
   * compileOnly} — never bundled — so relocating it is pure harm: the rewritten name exists
   * nowhere.
   */
  @Test
  public void redshiftVendorClassNameConstantsAreNotRelocated() throws Exception {
    String relation = "io.github.spark_redshift_community.spark.redshift.RedshiftRelation";
    String provider = "io.github.spark_redshift_community.spark.redshift.DefaultSource";

    assertConstantEquals(
        "io.acryl.shaded.io.openlineage.spark.agent.vendor.redshift.Constants",
        "REDSHIFT_CLASS_NAME",
        relation);
    assertConstantEquals(
        "io.acryl.shaded.io.openlineage.spark.agent.vendor.redshift.Constants",
        "REDSHIFT_PROVIDER_CLASS_NAME",
        provider);
    // The visitor keeps its own copy of the literal; fixing only Constants would still leave it
    // dead.
    assertConstantEquals(
        "io.acryl.shaded.io.openlineage.spark.agent.vendor.redshift.lifecycle.RedshiftRelationVisitor",
        "REDSHIFT_CLASS_NAME",
        relation);
  }

  /**
   * Asserts the exact canonical name rather than merely the absence of the {@code io.acryl.shaded.}
   * prefix: relocation always adds that prefix, so a prefix check catches the shading bug, but any
   * other wrong value (a typo, a rename upstream) would slip through unnoticed.
   */
  private static void assertConstantEquals(String className, String fieldName, String expected)
      throws Exception {
    Field field = Class.forName(className).getDeclaredField(fieldName);
    field.setAccessible(true);
    String value = (String) field.get(null);
    if (value.startsWith(SHADED_PREFIX)) {
      fail(
          className
              + "."
              + fieldName
              + " was rewritten by shading to '"
              + value
              + "' (issue #19005). It names a class the host classpath supplies, so loadClass()"
              + " can never resolve it and the Redshift visitor silently never fires.");
    }
    assertEquals(
        expected, value, className + "." + fieldName + " must name the host-supplied class");
  }
}
