package datahub.client.v2.entity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for entities that support domain assignment.
 *
 * <p>Entities implementing this interface can be assigned to a domain for organizational grouping.
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.setDomain("urn:li:domain:Marketing");
 * </pre>
 */
public interface Domainable {

  /**
   * Sets the domain for this entity.
   *
   * @param domainUrn the URN of the domain, or null to clear
   * @return this entity for method chaining
   */
  @Nonnull
  Entity setDomain(@Nullable String domainUrn);

  /**
   * Clears the domain assignment for this entity.
   *
   * @return this entity for method chaining
   */
  @Nonnull
  Entity clearDomain();
}
