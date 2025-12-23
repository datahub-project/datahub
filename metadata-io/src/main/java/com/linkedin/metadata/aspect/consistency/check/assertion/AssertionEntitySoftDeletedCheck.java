package com.linkedin.metadata.aspect.consistency.check.assertion;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Check that the entity referenced by an assertion is not soft-deleted.
 *
 * <p>If the referenced entity (assertee) is soft-deleted, the assertion should also be soft-deleted
 * to maintain consistency.
 */
@Component("consistencyAssertionEntitySoftDeletedCheck")
public class AssertionEntitySoftDeletedCheck extends AbstractAssertionCheck {

  public AssertionEntitySoftDeletedCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Entity Soft-Deleted";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the entity referenced by an assertion is not soft-deleted";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkAssertion(
      @Nonnull CheckContext ctx,
      @Nonnull Urn assertionUrn,
      @Nonnull EntityResponse response,
      @Nonnull AssertionInfo assertionInfo) {

    // Skip if no entityUrn (handled by AssertionEntityUrnMissingCheck)
    if (!assertionInfo.hasEntityUrn() || assertionInfo.getEntityUrn() == null) {
      return Collections.emptyList();
    }

    Urn entityUrn = assertionInfo.getEntityUrn();

    // Skip if invalid type (handled by AssertionEntityTypeInvalidCheck)
    if (!isValidAssertionEntityType(entityUrn.getEntityType())) {
      return Collections.emptyList();
    }

    // Skip if entity doesn't exist at all (handled by AssertionEntityNotFoundCheck)
    if (!referencedEntityExists(ctx, entityUrn)) {
      return Collections.emptyList();
    }

    // Check if entity is soft-deleted
    if (isReferencedEntitySoftDeleted(ctx, entityUrn)) {
      return List.of(
          createIssueBuilder(assertionUrn, ConsistencyFixType.SOFT_DELETE)
              .description(String.format("Assertion references soft-deleted entity: %s", entityUrn))
              .relatedUrns(List.of(entityUrn))
              .batchItems(List.of(createSoftDeleteItem(ctx, assertionUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }
}
