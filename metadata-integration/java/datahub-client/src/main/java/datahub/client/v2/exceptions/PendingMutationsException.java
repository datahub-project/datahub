package datahub.client.v2.exceptions;

import java.util.ConcurrentModificationException;
import javax.annotation.Nonnull;

/**
 * Exception thrown when attempting to read from an entity that has pending mutations.
 *
 * <p>This exception enforces a strict contract: after mutating an entity (via add*(), set*(),
 * remove*() methods), reads are not allowed until the entity has been saved to the server.
 *
 * <p><b>Why this restriction exists:</b>
 *
 * <ul>
 *   <li>Prevents reading potentially stale or incomplete cached data
 *   <li>Forces explicit save-then-fetch workflow for data consistency
 *   <li>Catches bugs where developers assume local mutations are immediately readable
 * </ul>
 *
 * <p><b>How to fix this error:</b>
 *
 * <pre>{@code
 * // Option 1: Save entity, then read from same object (dirty flag cleared)
 * Chart chart = Chart.builder().tool("looker").id("my_chart").build();
 * chart.setChartType("BAR");
 * client.entities().upsert(chart);  // Clears dirty flag
 * String type = chart.getChartType();  // Now works
 *
 * // Option 2: Read before mutating (preferred for conditional logic)
 * Chart chart = client.entities().get(chartUrn);
 * String existingType = chart.getChartType();  // Read first
 * if (existingType == null) {
 *     chart.setChartType("BAR");  // Then mutate
 *     client.entities().upsert(chart);
 * }
 *
 * // Option 3: Separate mutation and read operations
 * Chart chart = Chart.builder().tool("looker").id("my_chart").build();
 * chart.setChartType("BAR");
 * client.entities().upsert(chart);
 * // Later, in different code path:
 * Chart fetched = client.entities().get(chartUrn);
 * String type = fetched.getChartType();  // Safe
 * }</pre>
 *
 * <p><b>Design rationale:</b> This restriction can be relaxed in future versions to support
 * read-after-write for specific operation types (e.g., SET operations). Starting with strict
 * enforcement ensures safety and allows loosening later without breaking existing code.
 *
 * @see datahub.client.v2.entity.Entity#markDirty()
 * @see datahub.client.v2.entity.Entity#clearDirty()
 * @see datahub.client.v2.entity.Entity#checkNotDirty(String)
 */
public class PendingMutationsException extends ConcurrentModificationException {

  private final String entityType;
  private final String operation;

  /**
   * Constructs a PendingMutationsException with entity context.
   *
   * @param entityType the type of entity (e.g., "chart", "dataset")
   * @param operation the read operation that was attempted (e.g., "read chartType", "read tags")
   */
  public PendingMutationsException(@Nonnull String entityType, @Nonnull String operation) {
    super(buildMessage(entityType, operation));
    this.entityType = entityType;
    this.operation = operation;
  }

  /**
   * Returns the entity type on which the read was attempted.
   *
   * @return entity type (e.g., "chart", "dataset")
   */
  @Nonnull
  public String getEntityType() {
    return entityType;
  }

  /**
   * Returns the read operation that was attempted.
   *
   * @return operation description (e.g., "read chartType")
   */
  @Nonnull
  public String getOperation() {
    return operation;
  }

  private static String buildMessage(String entityType, String operation) {
    return String.format(
        "Cannot %s on %s: entity has pending mutations.\n"
            + "\n"
            + "This entity has been modified and reads are not available until after save.\n"
            + "\n"
            + "To fix this error:\n"
            + "\n"
            + "1. Save the entity first:\n"
            + "   client.entities().upsert(entity);\n"
            + "   // Now reads work (dirty flag cleared)\n"
            + "   entity.%s();  // Works after save\n"
            + "\n"
            + "2. Or read BEFORE mutating:\n"
            + "   %s entity = client.entities().get(urn);\n"
            + "   Object value = entity.%s();  // Read first\n"
            + "   entity.setSomeField(...);    // Then mutate\n"
            + "\n"
            + "3. Or separate mutation and read operations:\n"
            + "   entity.setSomeField(...);\n"
            + "   client.entities().upsert(entity);\n"
            + "   // Later, in different code:\n"
            + "   %s refreshed = client.entities().get(urn);\n"
            + "   refreshed.%s();  // Safe\n"
            + "\n"
            + "This restriction ensures data consistency and may be relaxed in future versions.",
        operation,
        entityType,
        operation.replace("read ", "get"),
        capitalize(entityType),
        operation.replace("read ", "get"),
        capitalize(entityType),
        operation.replace("read ", "get"));
  }

  private static String capitalize(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }
}
