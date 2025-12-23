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
 * Check that the entity referenced by an assertion exists.
 *
 * <p>If the referenced entity has been hard-deleted, the assertion is orphaned and should also be
 * hard-deleted to maintain referential integrity.
 */
@Component("consistencyAssertionEntityNotFoundCheck")
public class AssertionEntityNotFoundCheck extends AbstractAssertionCheck {

  public AssertionEntityNotFoundCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Entity Not Found";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that the entity referenced by an assertion exists (not hard-deleted)";
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

    // Check if entity exists (including soft-deleted)
    if (!referencedEntityExists(ctx, entityUrn)) {
      return List.of(
          createIssueBuilder(assertionUrn, ConsistencyFixType.HARD_DELETE)
              .description(String.format("Assertion references non-existent entity: %s", entityUrn))
              .relatedUrns(List.of(entityUrn))
              .hardDeleteUrns(List.of(assertionUrn))
              .build());
    }

    return Collections.emptyList();
  }
}
