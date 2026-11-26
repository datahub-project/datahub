package datahub.spark;

import static datahub.spark.ListenerConf.listener;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import org.apache.spark.sql.SaveMode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Upstream's {@code SparkOpenLineageExtensionVisitorWrapper} defines {@code
 * OpenLineageExtensionProvider} in every thread context class loader that cannot load it, so a
 * connector loaded by another class loader can implement the extension interfaces (OpenLineage
 * #3407). It does that by calling {@code defineClass} reflectively. For a {@code URLClassLoader}
 * (Spark's {@code MutableURLClassLoader} included) that resolves to {@code
 * SecureClassLoader.defineClass}, which on Java 17+ needs {@code
 * --add-opens=java.base/java.security}. Spark's launcher does not add that, and neither does this
 * suite, so here the injection fails.
 *
 * <p>A parked thread whose context class loader cannot see the provider makes the wrapper attempt
 * the injection. Whether or not it succeeds, the job's lineage must still be emitted.
 */
@Tag("integration")
public class SparkExtensionProviderInjectionSmokeTest extends SparkSmokeTestBase {

  private static final String PROVIDER =
      "io.openlineage.spark.extension.OpenLineageExtensionProvider";

  @Test
  public void lineageIsEmittedWhetherOrNotTheProviderCanBeInjected(@TempDir Path tmp)
      throws Exception {
    Path in = writeCsv(tmp.resolve("in.csv"), "id,v\n1,a\n");
    URLClassLoader isolated = new URLClassLoader(new URL[0], ClassLoader.getPlatformClassLoader());
    CountDownLatch release = new CountDownLatch(1);
    Thread parked =
        new Thread(
            () -> {
              try {
                release.await();
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            },
            "isolated-classloader-thread");
    parked.setContextClassLoader(isolated);
    parked.setDaemon(true);
    parked.start();

    EmittedMetadata md;
    try {
      md =
          runJob(
              listener().emitToFile(tmp.resolve("mcps.json")),
              spark ->
                  spark
                      .read()
                      .option("header", "true")
                      .csv(in.toString())
                      .write()
                      .mode(SaveMode.Overwrite)
                      .csv(tmp.resolve("out").toString()));
    } finally {
      release.countDown();
    }

    boolean javaSecurityOpen =
        Object.class.getModule().isOpen("java.security", getClass().getModule());
    assertEquals(
        javaSecurityOpen,
        canLoad(isolated, PROVIDER),
        "the provider should be injected exactly when java.security is open (open="
            + javaSecurityOpen
            + ")");
    assertTrue(
        md.outputDatasetUrns().stream().anyMatch(u -> u.contains("/out")),
        "lineage should be emitted even when the injection fails:\n" + md.raw);
  }

  private static boolean canLoad(ClassLoader loader, String className) {
    try {
      Class.forName(className, false, loader);
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }
}
