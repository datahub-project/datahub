package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * Regression guard for issue #18558. Runs against the real shaded {@code shadowJar_2_13} (see the
 * {@code sparkSmoke4RuntimeOnly} wiring in build.gradle), so it exercises the exact artifact users
 * deploy — unlike the unit/`sparkRealSmokeTest` suites, which run against the unshaded classpath
 * and therefore never see the relocation bug.
 *
 * <p>{@code OpenLineageSql.parse} is a {@code native} method whose entry point is compiled into a
 * Rust library as {@code Java_io_openlineage_sql_OpenLineageSql_parse}. Shading cannot rewrite
 * symbol names inside a native binary, so if the build relocates {@code io.openlineage.sql} the JVM
 * looks for {@code Java_io_acryl_shaded_..._parse} and this call throws {@link
 * UnsatisfiedLinkError} — the crash reported for Spark JDBC / Delta MERGE jobs.
 */
public class ShadedOpenLineageSqlJniTest {

  @Test
  public void nativeSqlParserResolvesInShadedJar() throws Exception {
    Class<?> cls;
    try {
      // Correct location: the JNI package must stay at its canonical name.
      cls = Class.forName("io.openlineage.sql.OpenLineageSql");
    } catch (ClassNotFoundException notCanonical) {
      // If it was relocated, load it there anyway so we still reach the native call and surface the
      // real failure mode (UnsatisfiedLinkError) rather than a misleading ClassNotFoundException.
      cls = Class.forName("io.acryl.shaded.io.openlineage.sql.OpenLineageSql");
    }

    Method parse = cls.getMethod("parse", java.util.List.class, String.class);
    Object result;
    try {
      result =
          parse.invoke(null, Collections.singletonList("SELECT a FROM my_db.my_table"), "postgres");
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof UnsatisfiedLinkError) {
        fail(
            "OpenLineageSql native parse is unresolved in the shaded jar (issue #18558): "
                + e.getCause().getMessage());
      }
      throw e;
    }

    assertTrue(result instanceof Optional, "parse should return an Optional<SqlMeta>");
    assertTrue(
        ((Optional<?>) result).isPresent(),
        "native SQL parser returned empty — the JNI library did not load");
  }
}
