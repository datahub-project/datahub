package datahub.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.TreeSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** A JDBC {@code dbtable} that joins two tables resolves to both of them. */
@Tag("integration")
public class SparkJdbcJoinDbtableSmokeTest extends JdbcDbtableSmokeTestBase {

  @Test
  public void joinDbtableResolvesToBothTables(@TempDir Path tmp) throws Exception {
    EmittedMetadata md =
        readDbtable(tmp, "public.orders o JOIN public.customers c ON o.customer_id = c.cust_id");

    assertEquals(
        new TreeSet<>(Arrays.asList(CUSTOMERS, ORDERS)),
        postgresInputs(md),
        "the join dbtable should resolve to both tables it reads:\n" + md.raw);
  }
}
