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
import java.util.Set;
import javax.annotation.Nonnull;
import org.springframework.stereotype.Component;

/**
 * Check that assertions reference a valid entity type.
 *
 * <p>Valid entity types are derived dynamically from @UrnValidation annotations in the
 * assertionInfo aspect PDL model via the entity registry.
 *
 * <p>Note: schemaField is excluded as a valid primary entity type since it's only used for
 * secondary targets. Assertions referencing invalid entity types should be soft-deleted.
 */
@Component("consistencyAssertionEntityTypeInvalidCheck")
public class AssertionEntityTypeInvalidCheck extends AbstractAssertionCheck {

  public AssertionEntityTypeInvalidCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Entity Type Invalid";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that assertions reference a valid entity type based on PDL annotations";
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
    String entityType = entityUrn.getEntityType();

    if (!isValidAssertionEntityType(entityType)) {
      Set<String> validTypes = getValidAssertionEntityTypes();
      return List.of(
          createIssueBuilder(assertionUrn, ConsistencyFixType.SOFT_DELETE)
              .description(
                  String.format(
                      "Assertion has invalid entityUrn type '%s' (expected one of: %s)",
                      entityType, String.join(", ", validTypes)))
              .relatedUrns(List.of(entityUrn))
              .batchItems(List.of(createSoftDeleteItem(ctx, assertionUrn, response)))
              .build());
    }

    return Collections.emptyList();
  }
}
