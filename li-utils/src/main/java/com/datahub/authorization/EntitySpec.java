package com.datahub.authorization;

import com.linkedin.data.template.RecordTemplate;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Details about the entities involved in the authorization process. It models the actor and the
 * resource being acted upon. Resource types currently supported can be found inside of {@link
 * com.linkedin.metadata.authorization.PoliciesConfig}
 */
@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class EntitySpec {
  /** The entity type. (dataset, chart, dashboard, corpGroup, etc). */
  @Nonnull private final String type;

  /**
   * The entity identity. Most often, this corresponds to the raw entity urn.
   * (urn:li:corpGroup:groupId)
   */
  @Nonnull private final String entity;

  /**
   * Optional map of proposed aspects that override database values during authorization.
   *
   * <p>This enables authorizing operations based on proposed changes before they're committed. For
   * example, when creating an entity with a domain, the domain aspect can be passed here so
   * PolicyEngine can authorize based on the domain being assigned (not the null/empty domain that
   * exists before creation).
   *
   * <p>Map key: aspect name (e.g., "domains", "ownership", "tags") Map value: aspect data as
   * RecordTemplate
   *
   * <p>FieldResolvers should check this map before fetching from the database.
   */
  @Nullable private final Map<String, RecordTemplate> proposedAspects;

  /** Convenience constructor for EntitySpec without proposed aspects. */
  public EntitySpec(@Nonnull String type, @Nonnull String entity) {
    this(type, entity, null);
  }
}
