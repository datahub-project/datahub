package datahub.client.v2.exceptions;

import javax.annotation.Nonnull;

/**
 * Exception thrown when attempting to mutate a read-only entity.
 *
 * <p>This exception enforces immutability for entities fetched from the server. After fetching an
 * entity, all mutation operations (set*(), add*(), remove*()) will throw this exception.
 *
 * <p><b>Why this restriction exists:</b>
 *
 * <ul>
 *   <li>Makes mutations explicit and intentional
 *   <li>Prevents accidental modification of fetched entities
 *   <li>Enables safe passing of entities to functions without risk of mutation
 *   <li>Clear separation between read and write workflows
 * </ul>
 *
 * <p><b>How to fix this error:</b>
 *
 * <pre>{@code
 * // Option 1: Create mutable copy before mutating (recommended for fetched entities)
 * Dataset dataset = client.entities().get(urn);  // Read-only
 * String desc = dataset.getDescription();         // Reads work fine
 *
 * Dataset mutable = dataset.mutable();            // Get mutable copy
 * mutable.setDescription("Updated");              // Now mutations work
 * client.entities().upsert(mutable);
 *
 * // Option 2: Use builder for new entities (already mutable)
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("table")
 *     .build();
 * dataset.setDescription("New description");      // Works - builder entities are mutable
 * client.entities().upsert(dataset);
 * }</pre>
 *
 * <p><b>Design rationale:</b> This immutability-by-default pattern is common in modern APIs (Rust's
 * ownership model, Python's frozen dataclasses, Java's immutable collections). It makes code safer
 * and mutation intent explicit.
 *
 * @see datahub.client.v2.entity.Entity#mutable()
 * @see datahub.client.v2.entity.Entity#isMutable()
 * @see datahub.client.v2.entity.Entity#isReadOnly()
 */
public class ReadOnlyEntityException extends UnsupportedOperationException {

  private final String entityType;
  private final String operation;

  /**
   * Constructs a ReadOnlyEntityException with entity context.
   *
   * @param entityType the type of entity (e.g., "chart", "dataset")
   * @param operation the mutation operation that was attempted (e.g., "set description", "add tag")
   */
  public ReadOnlyEntityException(@Nonnull String entityType, @Nonnull String operation) {
    super(buildMessage(entityType, operation));
    this.entityType = entityType;
    this.operation = operation;
  }

  /**
   * Returns the entity type on which the mutation was attempted.
   *
   * @return entity type (e.g., "chart", "dataset")
   */
  @Nonnull
  public String getEntityType() {
    return entityType;
  }

  /**
   * Returns the mutation operation that was attempted.
   *
   * @return operation description (e.g., "set description")
   */
  @Nonnull
  public String getOperation() {
    return operation;
  }

  private static String buildMessage(String entityType, String operation) {
    return String.format(
        "Cannot %s on %s: entity is read-only.\n"
            + "\n"
            + "This entity was fetched from the server and is immutable by default.\n"
            + "\n"
            + "To fix this error:\n"
            + "\n"
            + "1. Create a mutable copy:\n"
            + "   %s mutable = entity.mutable();\n"
            + "   mutable.%s();  // Now works\n"
            + "   client.entities().upsert(mutable);\n"
            + "\n"
            + "2. Or use builder for new entities:\n"
            + "   %s entity = %s.builder()\n"
            + "       .id(\"...\")\n"
            + "       .build();\n"
            + "   entity.%s();  // Works - builder entities are mutable\n"
            + "\n"
            + "Why this restriction? Immutability by default makes mutations explicit,\n"
            + "prevents accidental changes, and enables safe passing of entities between functions.\n",
        operation,
        entityType,
        capitalize(entityType),
        operation,
        capitalize(entityType),
        capitalize(entityType),
        operation);
  }

  private static String capitalize(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }
}
