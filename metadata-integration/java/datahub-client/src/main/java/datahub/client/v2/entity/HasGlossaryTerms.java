package datahub.client.v2.entity;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.GlossaryTermAssociation;
import com.linkedin.common.GlossaryTermAssociationArray;
import com.linkedin.common.GlossaryTerms;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.GlossaryTermsPatchBuilder;
import datahub.event.MetadataChangeProposalWrapper;
import java.net.URISyntaxException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/**
 * Mixin interface providing glossary term operations for entities that support glossary terms.
 *
 * <p>Entities implementing this interface can have glossary terms attached for semantic meaning and
 * governance.
 *
 * <p>Example usage:
 *
 * <pre>
 * Dataset dataset = Dataset.builder()
 *     .platform("snowflake")
 *     .name("my_table")
 *     .build();
 *
 * dataset.addTerm("urn:li:glossaryTerm:CustomerData");
 * dataset.addTerm("urn:li:glossaryTerm:SensitiveInfo");
 * </pre>
 *
 * @param <T> the concrete entity type (self-bounded generic for fluent returns)
 */
public interface HasGlossaryTerms<T extends Entity & HasGlossaryTerms<T>> {

  /**
   * Adds a glossary term to this entity.
   *
   * @param termUrn the URN of the glossary term
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addTerm(@Nonnull String termUrn) {
    try {
      return addTerm(GlossaryTermUrn.createFromString(termUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(termUrn, e);
    }
  }

  /**
   * Adds a glossary term to this entity.
   *
   * @param termUrn the glossary term URN object
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T addTerm(@Nonnull GlossaryTermUrn termUrn) {
    Entity entity = (Entity) this;

    // Get or create accumulated patch builder for glossaryTerms
    GlossaryTermsPatchBuilder builder =
        entity.getPatchBuilder("glossaryTerms", GlossaryTermsPatchBuilder.class);
    if (builder == null) {
      builder = new GlossaryTermsPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("glossaryTerms", builder);
    }

    builder.addTerm(termUrn, null);
    return (T) this;
  }

  /**
   * Removes a glossary term from this entity.
   *
   * @param termUrn the URN of the glossary term to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeTerm(@Nonnull String termUrn) {
    try {
      return removeTerm(GlossaryTermUrn.createFromString(termUrn));
    } catch (URISyntaxException e) {
      throw new datahub.client.v2.exceptions.InvalidUrnException(termUrn, e);
    }
  }

  /**
   * Removes a glossary term from this entity.
   *
   * @param termUrn the glossary term URN object to remove
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T removeTerm(@Nonnull GlossaryTermUrn termUrn) {
    Entity entity = (Entity) this;

    // If the glossaryTerms aspect is cached (entity was fetched from server),
    // we need to remove it from cache so the patch can take effect
    entity.removeAspect(com.linkedin.common.GlossaryTerms.class);

    // Get or create accumulated patch builder for glossaryTerms
    GlossaryTermsPatchBuilder builder =
        entity.getPatchBuilder("glossaryTerms", GlossaryTermsPatchBuilder.class);
    if (builder == null) {
      builder = new GlossaryTermsPatchBuilder().urn(entity.getUrn());
      entity.registerPatchBuilder("glossaryTerms", builder);
    }

    builder.removeTerm(termUrn);
    return (T) this;
  }

  /**
   * Sets the complete list of glossary terms for this entity, replacing any existing terms.
   *
   * <p>Unlike {@link #addTerm(String)} which creates a patch, this method creates a full aspect
   * replacement. This ensures that ONLY the specified terms will be present after save.
   *
   * @param termUrns the list of glossary term URNs to set
   * @return this entity for fluent chaining
   */
  @Nonnull
  default T setTerms(@Nonnull List<String> termUrns) {
    Entity entity = (Entity) this;

    // Convert term strings to GlossaryTermUrn objects
    List<GlossaryTermUrn> terms =
        termUrns.stream()
            .map(
                termUrn -> {
                  try {
                    return GlossaryTermUrn.createFromString(termUrn);
                  } catch (URISyntaxException e) {
                    throw new datahub.client.v2.exceptions.InvalidUrnException(termUrn, e);
                  }
                })
            .collect(Collectors.toList());

    // Create full GlossaryTerms aspect with complete list
    GlossaryTerms glossaryTerms = new GlossaryTerms();
    GlossaryTermAssociationArray termArray = new GlossaryTermAssociationArray();

    for (GlossaryTermUrn termUrn : terms) {
      GlossaryTermAssociation termAssoc = new GlossaryTermAssociation();
      termAssoc.setUrn(termUrn);
      termArray.add(termAssoc);
    }

    glossaryTerms.setTerms(termArray);

    // Set audit stamp
    AuditStamp auditStamp = new AuditStamp();
    auditStamp.setActor(UrnUtils.getUrn("urn:li:corpuser:datahub"));
    auditStamp.setTime(System.currentTimeMillis());
    glossaryTerms.setAuditStamp(auditStamp);

    // Create MCP wrapper for full aspect replacement
    MetadataChangeProposalWrapper mcp =
        MetadataChangeProposalWrapper.builder()
            .entityType(entity.getEntityType())
            .entityUrn(entity.getUrn())
            .upsert()
            .aspect(glossaryTerms)
            .build();

    // Add to pending MCPs (this clears any patches for glossaryTerms)
    entity.addPendingMCP(mcp);

    return (T) this;
  }

  /**
   * Gets the list of glossary terms associated with this entity.
   *
   * @return the list of glossary term associations, or null if no terms are present
   */
  default List<GlossaryTermAssociation> getTerms() {
    Entity entity = (Entity) this;
    GlossaryTerms aspect = entity.getAspectLazy(GlossaryTerms.class);
    return aspect != null && aspect.hasTerms() ? aspect.getTerms() : null;
  }
}
