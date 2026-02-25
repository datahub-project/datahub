package datahub.client.v2.entity;

import com.linkedin.common.urn.Urn;
import java.net.URISyntaxException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Mixin interface providing container operations for entities that support container hierarchy.
 *
 * <p>Entities implementing this interface can be assigned to a parent container for hierarchical
 * organization (e.g., datasets within a database, charts within a dashboard).
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.setContainer("urn:li:container:(urn:li:dataPlatform:snowflake,mydb,PROD)");
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasContainer<T extends Entity & HasContainer<T>> {

  /**
   * Sets the parent container for this entity.
   *
   * @param containerUrn the URN of the parent container, or null to clear
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setContainer(@Nullable String containerUrn) {
    if (containerUrn == null) {
      return clearContainer();
    }

    try {
      return setContainer(Urn.createFromString(containerUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(containerUrn, e);
    }
  }

  /**
   * Sets the parent container for this entity.
   *
   * @param containerUrn the URN of the parent container
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setContainer(@Nonnull Urn containerUrn) {
    Entity entity = (Entity) this;
    com.linkedin.container.Container containerAspect = new com.linkedin.container.Container();
    containerAspect.setContainer(containerUrn);

    String aspectName = entity.getAspectName(com.linkedin.container.Container.class);
    entity.cache.put(aspectName, containerAspect, AspectSource.LOCAL, true);
    return (T) this;
  }

  /**
   * Clears the parent container assignment for this entity.
   *
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T clearContainer() {
    Entity entity = (Entity) this;
    String aspectName = entity.getAspectName(com.linkedin.container.Container.class);
    entity.cache.remove(aspectName);
    return (T) this;
  }
}
