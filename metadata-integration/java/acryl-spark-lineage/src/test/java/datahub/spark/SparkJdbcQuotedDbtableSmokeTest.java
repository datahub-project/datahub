package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Collections;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins a known gap: a JDBC {@code dbtable} written with quoted identifiers has no input.
 * OpenLineage (1.53 through 1.55) names the dataset {@code wh."public"."customers"}, keeping the
 * quotes, and the converter cannot build a URI from that name, so it drops the input. This is the
 * same with and without DataHub's former vendored {@code JdbcSparkUtils}.
 *
 * <p>When an OpenLineage upgrade makes this test fail, check the new input and, if it is {@link
 * #CUSTOMERS}, change the expectation to it.
 */
@Tag("integration")
public class SparkJdbcQuotedDbtableSmokeTest extends JdbcDbtableSmokeTestBase {

  @Test
  public void quotedDbtableHasNoInput(@TempDir Path tmp) throws Exception {
    EmittedMetadata md = readDbtable(tmp, "\"public\".\"customers\"");

    assertEquals(
        Collections.emptySet(),
        postgresInputs(md),
        "the quoted-dbtable input changed; see this class's javadoc:\n" + md.raw);
  }
}
