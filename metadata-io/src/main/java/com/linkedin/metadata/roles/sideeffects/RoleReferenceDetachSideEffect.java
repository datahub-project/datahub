package com.linkedin.metadata.roles.sideeffects;

import static com.linkedin.metadata.Constants.ACCESS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.ROLE_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ROLE_KEY;

import com.datahub.util.RecordUtils;
import com.linkedin.common.Access;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.hooks.ReferenceDetachSideEffect;
import com.linkedin.metadata.aspect.patch.GenericJsonPatch;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/** When a Role is hard-deleted, strip that Role URN from every entity's {@code access} aspect. */
public class RoleReferenceDetachSideEffect extends ReferenceDetachSideEffect {

  public static final String ROLES_SEARCH_FIELD = "roles";

  private static final String ROLES_FIELD = "roles";
  private static final String ROLE_URN_FIELD = "urn";

  @Nonnull
  @Override
  protected String deletedEntityName() {
    return ROLE_ENTITY_NAME;
  }

  @Nonnull
  @Override
  protected String deletedKeyAspectName() {
    return ROLE_KEY;
  }

  @Nonnull
  @Override
  protected String referencingAspectName() {
    return ACCESS_ASPECT_NAME;
  }

  @Nonnull
  @Override
  protected String searchFieldName() {
    return ROLES_SEARCH_FIELD;
  }

  @Override
  protected boolean references(@Nonnull Aspect aspect, @Nonnull Urn deletedUrn) {
    Access access = RecordUtils.toRecordTemplate(Access.class, aspect.data());
    return access.getRoles() != null
        && access.getRoles().stream().anyMatch(role -> deletedUrn.equals(role.getUrn()));
  }

  @Nonnull
  @Override
  protected GenericJsonPatch removalPatch(@Nonnull Urn deletedUrn) {
    GenericJsonPatch.PatchOp patchOp = new GenericJsonPatch.PatchOp();
    patchOp.setOp(PatchOperationType.REMOVE.getValue());
    patchOp.setPath("/" + ROLES_FIELD + "/" + pathSegment(deletedUrn));
    return GenericJsonPatch.builder()
        .arrayPrimaryKeys(Map.of(ROLES_FIELD, List.of(ROLE_URN_FIELD)))
        .patch(List.of(patchOp))
        .build();
  }
}
