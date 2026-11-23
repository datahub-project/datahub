package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Collections;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A JDBC {@code dbtable} that aliases a table resolves to that table. */
@Tag("integration")
public class SparkJdbcAliasDbtableSmokeTest extends JdbcDbtableSmokeTestBase {

  @Test
  public void aliasedDbtableResolvesToItsTable(@TempDir Path tmp) throws Exception {
    EmittedMetadata md = readDbtable(tmp, "public.orders AS o");

    assertEquals(
        Collections.singleton(ORDERS),
        postgresInputs(md),
        "the aliased dbtable should resolve to the table it reads:\n" + md.raw);
  }
}
