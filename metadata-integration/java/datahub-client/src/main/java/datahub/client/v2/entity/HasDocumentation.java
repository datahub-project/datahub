package datahub.client.v2.entity;

import com.linkedin.common.Documentation;
import com.linkedin.common.DocumentationAssociation;
import com.linkedin.common.DocumentationAssociationArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.builder.DocumentationPatchBuilder;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Mixin interface providing documentation operations for entities that support documentation.
 *
 * <p>Entities implementing this interface can have documentation entries assigned, with optional
 * attribution to a source URN. Multiple documentation entries from different sources can coexist.
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.addDocumentation("This table contains customer transaction data.");
 * dataset.addDocumentation("Automated doc from lineage.", attributionUrn);
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasDocumentation<T extends Entity & HasDocumentation<T>> {

  /**
   * Adds an unattributed documentation entry to this entity.
   *
   * @param text the documentation text
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addDocumentation(@Nonnull String text) {
    Entity entity = (Entity) this;

    DocumentationPatchBuilder builder =
        entity.getPatchBuilder("documentation", DocumentationPatchBuilder.class);
    if (builder == null) {
      builder = new DocumentationPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("documentation", builder);
    }

    builder.addDocumentation(text);
    return (T) this;
  }

  /**
   * Removes the documentation entry for a specific attribution source.
   *
   * @param attributionSource the source URN whose documentation entry to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeDocumentation(@Nonnull Urn attributionSource) {
    Entity entity = (Entity) this;

    DocumentationPatchBuilder builder =
        entity.getPatchBuilder("documentation", DocumentationPatchBuilder.class);
    if (builder == null) {
      builder = new DocumentationPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("documentation", builder);
    }

    builder.removeDocumentation(attributionSource);
    return (T) this;
  }

  /**
   * Removes all documentation entries from this entity.
   *
   * <p>Uses {@code *} as the source wildcard, which the backend expands to every source — removing
   * all documentation entries without affecting other aspects.
   *
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeAllDocumentation() {
    Entity entity = (Entity) this;

    DocumentationPatchBuilder builder =
        entity.getPatchBuilder("documentation", DocumentationPatchBuilder.class);
    if (builder == null) {
      builder = new DocumentationPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("documentation", builder);
    }

    builder.removeDocumentation();
    return (T) this;
  }

  /**
   * Sets the complete list of documentation entries for this entity, replacing any existing ones.
   *
   * <p>Unlike {@link #addDocumentation(String)} which creates a patch, this method creates a full
   * aspect replacement. This ensures that ONLY the specified documentation entries will be present
   * after save.
   *
   * @param documentationTexts the list of documentation strings to set (all unattributed)
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setDocumentations(@Nonnull List<String> documentationTexts) {
    Entity entity = (Entity) this;

    Documentation documentation = new Documentation();
    DocumentationAssociationArray docsArray = new DocumentationAssociationArray();

    for (String text : documentationTexts) {
      DocumentationAssociation assoc = new DocumentationAssociation();
      assoc.setDocumentation(text);
      docsArray.add(assoc);
    }

    documentation.setDocumentations(docsArray);

    MetadataChangeProposalWrapper mcp =
        MetadataChangeProposalWrapper.builder()
            .entityType(entity.getEntityType())
            .entityUrn(entity.getUrn())
            .upsert()
            .aspect(documentation)
            .build();

    entity.addPendingMCP(mcp);

    return (T) this;
  }

  /**
   * Gets the list of documentation associations for this entity.
   *
   * @return the list of documentation associations, or null if no documentation is present
   */
  @Nullable
  default List<DocumentationAssociation> getDocumentations() {
    Entity entity = (Entity) this;
    Documentation aspect = entity.getAspectLazy(Documentation.class);
    return aspect != null && aspect.hasDocumentations() ? aspect.getDocumentations() : null;
  }
}
