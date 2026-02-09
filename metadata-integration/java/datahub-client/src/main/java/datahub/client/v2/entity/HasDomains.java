package datahub.client.v2.entity;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import datahub.client.v2.annotations.RequiresMutable;
import java.net.URISyntaxException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mixin interface providing domain operations for entities that support domain assignment.
 *
 * <p>Entities implementing this interface get all domain-related methods with proper return types
 * for fluent API chaining.
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasDomains<T extends Entity & HasDomains<T>> {

  Logger log = LoggerFactory.getLogger(HasDomains.class);
  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Sets the domain for this entity.
   *
   * @param domainUrn the domain URN string (must not be null)
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setDomain(@Nonnull String domainUrn) {
    try {
      return setDomain(Urn.createFromString(domainUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(domainUrn, e);
    }
  }

  /**
   * Sets the domain for this entity.
   *
   * @param domain the domain URN object (must not be null)
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setDomain(@Nonnull Urn domain) {
    Entity entity = (Entity) this;

    // Get or create Domains aspect
    Domains domains = entity.getOrCreateAspect(Domains.class);

    // Set single domain (replaces any existing domains)
    UrnArray domainArray = new UrnArray();
    domainArray.add(domain);
    domains.setDomains(domainArray);

    // Mark aspect as dirty since we modified it
    entity.markAspectDirty(Domains.class);

    return (T) this;
  }

  /**
   * Removes a specific domain from this entity.
   *
   * @param domainUrn the domain URN string to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeDomain(@Nonnull String domainUrn) {
    try {
      return removeDomain(Urn.createFromString(domainUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(domainUrn, e);
    }
  }

  /**
   * Removes a specific domain from this entity.
   *
   * <p>Note: This method removes the domain from the in-memory MCP. If you need to remove a domain
   * from an existing entity on the server, use clearDomains() to remove all domains.
   *
   * @param domain the domain URN object to remove
   * @return this entity for fluent chaining
   */
  @RequiresMutable
  @Nonnull
  default T removeDomain(@Nonnull Urn domain) {
    Entity entity = (Entity) this;
    entity.checkNotReadOnly("remove domain");

    // Get existing domains aspect (don't create if it doesn't exist)
    Domains domains = entity.getAspectCached(Domains.class);

    if (domains != null) {
      // Remove the specified domain
      UrnArray domainArray = domains.getDomains();
      domainArray.removeIf(urn -> urn.equals(domain));
      domains.setDomains(domainArray);

      // Mark aspect as dirty since we modified it in-place
      entity.markAspectDirty(Domains.class);
    }
    // If no domains aspect exists, nothing to remove

    return (T) this;
  }

  /**
   * Clears all domains from this entity.
   *
   * @return this entity for fluent chaining
   */
  @RequiresMutable
  @Nonnull
  default T clearDomains() {
    Entity entity = (Entity) this;
    entity.checkNotReadOnly("clear domains");

    // Get or create Domains aspect and set to empty array
    Domains domains = entity.getOrCreateAspect(Domains.class);
    domains.setDomains(new UrnArray());

    // Mark aspect as dirty since we modified it
    entity.markAspectDirty(Domains.class);

    return (T) this;
  }

  /**
   * Gets the first domain associated with this entity.
   *
   * <p>This is a convenience method for the common case where an entity has a single domain. If
   * multiple domains are present, only the first is returned.
   *
   * @return the first domain URN, or null if no domains are present
   */
  @Nullable
  default Urn getDomain() {
    Entity entity = (Entity) this;
    Domains aspect = entity.getAspectLazy(Domains.class);
    if (aspect != null && aspect.hasDomains() && !aspect.getDomains().isEmpty()) {
      return aspect.getDomains().get(0);
    }
    return null;
  }

  /**
   * Gets all domains associated with this entity.
   *
   * @return the list of domain URNs, or null if no domains are present
   */
  @Nullable
  default List<Urn> getDomains() {
    Entity entity = (Entity) this;
    Domains aspect = entity.getAspectLazy(Domains.class);
    return aspect != null && aspect.hasDomains() ? aspect.getDomains() : null;
  }
}
