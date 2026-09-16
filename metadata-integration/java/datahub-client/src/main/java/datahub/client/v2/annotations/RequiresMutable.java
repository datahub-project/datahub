package datahub.client.v2.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods that require a mutable entity.
 *
 * <p>This annotation serves two purposes:
 *
 * <ol>
 *   <li><b>Documentation:</b> Makes it immediately clear that the method requires a mutable entity
 *   <li><b>Test Enforcement:</b> Automated tests verify that all annotated methods properly call
 *       validation methods like {@link datahub.client.v2.entity.Entity#checkNotReadOnly(String)}
 * </ol>
 *
 * <p>Methods annotated with {@code @RequiresMutable} must manually call the appropriate check
 * method (e.g., {@code checkNotReadOnly("operation")}). The test suite enforces this contract via
 * bytecode inspection.
 *
 * <p>By default, this indicates that {@link Check#NOT_READ_ONLY} validation is required. Future
 * versions may support additional checks via the {@link #checks()} parameter.
 *
 * <p><b>Example usage:</b>
 *
 * <pre>{@code
 * @RequiresMutable
 * public Chart setDescription(@Nonnull String description) {
 *     checkNotReadOnly("set description");  // Required check
 *     ChartInfoPatchBuilder patch = ...
 *     return this;
 * }
 *
 * @RequiresMutable(operation = "add custom property")
 * public Chart addCustomProperty(@Nonnull String key, @Nonnull String value) {
 *     checkNotReadOnly("add custom property");
 *     // Custom operation name documents intent
 * }
 *
 * // Future: Multiple checks
 * @RequiresMutable(checks = {Check.NOT_READ_ONLY, Check.NOT_DIRTY})
 * public Chart strictMethod() {
 *     checkNotReadOnly("strict method");
 *     checkNotDirty("strict method");
 *     // Both checks required
 * }
 * }</pre>
 *
 * @see datahub.client.v2.entity.Entity#mutable()
 * @see datahub.client.v2.entity.Entity#isMutable()
 * @see datahub.client.v2.entity.Entity#checkNotReadOnly(String)
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RequiresMutable {

  /**
   * Custom operation description for error messages.
   *
   * <p>If empty, the operation name is automatically derived from the method name. For example,
   * "setDescription" becomes "set description".
   *
   * @return operation description, or empty string to auto-derive
   */
  String operation() default "";

  /**
   * Validation checks to perform before executing the method.
   *
   * <p>Default is {@link Check#NOT_READ_ONLY}, which ensures the entity is mutable (not fetched
   * from server without calling {@link datahub.client.v2.entity.Entity#mutable()}).
   *
   * <p>Future versions may add additional check types (e.g., NOT_DIRTY, IN_TRANSACTION).
   *
   * @return array of checks to perform
   */
  Check[] checks() default {Check.NOT_READ_ONLY};

  /**
   * Types of validation checks that can be performed on entities.
   *
   * <p>This enum is designed to be extended in future versions with additional check types without
   * breaking existing code. The default check is {@link #NOT_READ_ONLY}.
   */
  enum Check {
    /**
     * Validates that the entity is not read-only.
     *
     * <p>Read-only entities are those fetched from the server. To mutate a fetched entity, you must
     * first call {@link datahub.client.v2.entity.Entity#mutable()} to obtain a writable copy.
     *
     * <p>Throws {@link datahub.client.v2.exceptions.ReadOnlyEntityException} if entity is
     * read-only.
     */
    NOT_READ_ONLY

    // Future check types can be added here:
    // NOT_DIRTY,           // Entity has no pending mutations
    // IN_TRANSACTION,      // Operation is within a transaction context
    // HAS_PERMISSION,      // User has permission for this operation
    // FIELD_NOT_DIRTY      // Specific field has not been mutated
  }
}
