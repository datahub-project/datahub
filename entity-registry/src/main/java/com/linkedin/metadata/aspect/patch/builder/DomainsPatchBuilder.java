package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class DomainsPatchBuilder extends AbstractMultiFieldPatchBuilder<DomainsPatchBuilder> {

  private static final String BASE_PATH = "/domains";

  public DomainsPatchBuilder setDomain(@Nonnull Urn urn) {
    // remove existing list of domains to set the new one
    this.pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH, null));
    // add empty domains array
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.ADD.getValue(), BASE_PATH, instance.arrayNode()));
    // set the new domain
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            BASE_PATH + "/" + urn,
            instance.textNode(urn.toString())));
    return this;
  }

  public DomainsPatchBuilder removeDomain(@Nonnull Urn urn) {
    pathValues.add(
        ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH + "/" + urn, null));
    return this;
  }

  /**
   * Clears all domains from the entity.
   *
   * @return this builder
   */
  public DomainsPatchBuilder clearDomains() {
    pathValues.add(ImmutableTriple.of(PatchOperationType.REMOVE.getValue(), BASE_PATH, null));
    return this;
  }

  @Override
  protected String getAspectName() {
    return DOMAINS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
