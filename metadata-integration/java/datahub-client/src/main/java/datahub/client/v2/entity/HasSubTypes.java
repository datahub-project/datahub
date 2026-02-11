package datahub.client.v2.entity;

import com.linkedin.common.SubTypes;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Mixin interface providing subtype operations for entities that support subtypes.
 *
 * <p>SubTypes allow specializing a generic entity (e.g., making a Dataset also be a View or
 * LookerExplore).
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.setSubTypes("view", "materialized");
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasSubTypes<T extends Entity & HasSubTypes<T>> {

  /**
   * Sets the subtypes for this entity, replacing any existing subtypes.
   *
   * <p>This method creates a full aspect replacement, ensuring that ONLY the specified subtypes
   * will be present after save.
   *
   * @param typeNames the list of subtype names to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setSubTypes(@Nonnull String... typeNames) {
    return setSubTypes(Arrays.asList(typeNames));
  }

  /**
   * Sets the subtypes for this entity, replacing any existing subtypes.
   *
   * <p>This method creates a full aspect replacement, ensuring that ONLY the specified subtypes
   * will be present after save.
   *
   * @param typeNames the list of subtype names to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setSubTypes(@Nonnull List<String> typeNames) {
    Entity entity = (Entity) this;

    SubTypes subTypes = new SubTypes();
    com.linkedin.data.template.StringArray typeNamesArray =
        new com.linkedin.data.template.StringArray();
    typeNamesArray.addAll(typeNames);
    subTypes.setTypeNames(typeNamesArray);

    MetadataChangeProposalWrapper mcp =
        MetadataChangeProposalWrapper.builder()
            .entityType(entity.getEntityType())
            .entityUrn(entity.getUrn())
            .upsert()
            .aspect(subTypes)
            .build();

    entity.addPendingMCP(mcp);

    return (T) this;
  }
}
