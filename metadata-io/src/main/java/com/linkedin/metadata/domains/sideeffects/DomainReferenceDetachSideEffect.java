package com.linkedin.metadata.domains.sideeffects;

import static com.linkedin.metadata.Constants.DOMAINS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DOMAIN_KEY_ASPECT_NAME;

import com.datahub.util.RecordUtils;
import com.linkedin.common.urn.Urn;
import com.linkedin.domain.Domains;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.hooks.ReferenceDetachSideEffect;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * When a Domain is hard-deleted, strip that Domain URN from every entity's {@code domains} aspect.
 *
 * <p>Patches are removal-only and touch {@code domains} only; {@code DomainsSyncMutationHook}
 * re-derives {@code domainAssociations}.
 */
public class DomainReferenceDetachSideEffect extends ReferenceDetachSideEffect {

  public static final String DOMAINS_SEARCH_FIELD = "domains";

  @Nonnull
  @Override
  protected String deletedEntityName() {
    return DOMAIN_ENTITY_NAME;
  }

  @Nonnull
  @Override
  protected String deletedKeyAspectName() {
    return DOMAIN_KEY_ASPECT_NAME;
  }

  @Nonnull
  @Override
  protected String referencingAspectName() {
    return DOMAINS_ASPECT_NAME;
  }

  @Nonnull
  @Override
  protected String searchFieldName() {
    return DOMAINS_SEARCH_FIELD;
  }

  @Override
  protected boolean references(@Nonnull Aspect aspect, @Nonnull Urn deletedUrn) {
    Domains domains = RecordUtils.toRecordTemplate(Domains.class, aspect.data());
    return domains.getDomains() != null && domains.getDomains().contains(deletedUrn);
  }

  @Nonnull
  @Override
  protected GenericJsonPatch removalPatch(@Nonnull Urn deletedUrn) {
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(PatchOperationType.REMOVE.getValue());
    patchOp.setPath("/domains/" + pathSegment(deletedUrn));
    return GenericJsonPatch.builder().patch(List.of(patchOp)).build();
  }
}
