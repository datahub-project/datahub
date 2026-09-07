package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import org.junit.jupiter.api.Test;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Guards how the shaded agent participates in the host's logging.
 *
 * <p>The agent used to bundle slf4j-api plus a {@code reload4j} SLF4J binding and a root {@code
 * log4j.properties}, all unrelocated. On a Spark cluster that means two bindings on one classpath —
 * Spark ships {@code log4j-slf4j2-impl} — and SLF4J picks one nondeterministically. If the agent's
 * won, the bundled {@code log4j.properties} (which sets {@code rootLogger=INFO, STDOUT}) could
 * reconfigure the whole application's logging. The agent must instead bind to whatever the host
 * provides, so its own logs land in the user's configured Spark logs.
 *
 * <p>Runs against the shaded {@code shadowJar_2_13}. The complementary packaging assertions — that
 * the jar itself carries no {@code org/slf4j/} classes, no provider service file and no root {@code
 * log4j.properties} — live in {@code scripts/check_jar.sh}: here the classloader also sees Spark's
 * classpath, which legitimately supplies a binding, so jar contents cannot be asserted from inside
 * the JVM.
 */
public class ShadedLoggingBindingTest {

  @Test
  public void agentBindsToTheHostSlf4jProvider() {
    ILoggerFactory factory = LoggerFactory.getILoggerFactory();
    String impl = factory.getClass().getName();

    assertFalse(
        impl.toLowerCase().contains("reload4j"),
        "SLF4J bound to the agent's own bundled reload4j binding ("
            + impl
            + ") instead of the host's. The agent must not ship a binding — it hijacks the"
            + " application's logging configuration.");
    assertFalse(
        impl.contains("NOPLoggerFactory"),
        "SLF4J resolved no provider at all ("
            + impl
            + "), so the agent's logs go nowhere. Removing the bundled binding must leave the host's"
            + " binding in place, not eliminate logging entirely.");

    // Prove logging is actually usable, not merely bound.
    Logger log = LoggerFactory.getLogger(ShadedLoggingBindingTest.class);
    log.info("shaded agent logging is wired to the host binding: {}", impl);
  }

  /**
   * Nothing else on this classpath provides reload4j — Spark uses log4j2 — so the class resolving
   * at all means our jar is still shipping the binding.
   */
  @Test
  public void agentShipsNoReload4jBinding() {
    for (String cls :
        new String[] {
          "org.slf4j.reload4j.Reload4jServiceProvider", "org.slf4j.reload4j.Reload4jLoggerFactory"
        }) {
      try {
        Class.forName(cls);
        fail(
            cls
                + " resolved, so the shaded jar still bundles the reload4j SLF4J binding. Spark"
                + " supplies its own binding; shipping a second one makes which config wins"
                + " classloader-order dependent.");
      } catch (ClassNotFoundException expected) {
        // Correct — only the host's binding is present.
      }
    }
  }
}
