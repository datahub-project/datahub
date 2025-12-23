package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import com.linkedin.metadata.aspect.hooks.AssertionInfoMutator;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.GenericRecordUtils;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Check that assertions have an entityUrn defined.
 *
 * <p>An assertion without an entityUrn cannot determine what entity it asserts on. This check
 * attempts to derive the entityUrn from the assertion's type-specific properties (e.g.,
 * datasetAssertion, freshnessAssertion). If derivation succeeds, the assertion is fixed via upsert;
 * otherwise, it is soft-deleted.
 */
@Slf4j
@Component("consistencyAssertionEntityUrnMissingCheck")
public class AssertionEntityUrnMissingCheck extends AbstractAssertionCheck {

  public AssertionEntityUrnMissingCheck(@Nonnull EntityRegistry entityRegistry) {
    super(entityRegistry);
  }

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Entity URN Missing";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Checks that assertions have an entityUrn defined, attempting to derive it from type-specific properties if missing";
  }

  @Override
  @Nonnull
  protected List<ConsistencyIssue> checkAssertion(
      @Nonnull CheckContext ctx,
      @Nonnull Urn assertionUrn,
      @Nonnull EntityResponse response,
      @Nonnull AssertionInfo assertionInfo) {

    if (!assertionInfo.hasEntityUrn() || assertionInfo.getEntityUrn() == null) {
      // Try to derive entityUrn from the assertion's type-specific properties
      try {
        Urn derivedEntityUrn = AssertionInfoMutator.getEntityFromAssertionInfo(assertionInfo);
        if (derivedEntityUrn != null) {
          // Create a copy and set the derived entityUrn
          AssertionInfo fixedInfo = GenericRecordUtils.copy(assertionInfo, AssertionInfo.class);
          fixedInfo.setEntityUrn(derivedEntityUrn);

          BatchItem upsertItem =
              createUpsertItem(
                  ctx,
                  assertionUrn,
                  ASSERTION_INFO_ASPECT_NAME,
                  fixedInfo,
                  getAspect(response, ASSERTION_INFO_ASPECT_NAME));

          return List.of(
              createIssueBuilder(assertionUrn, ConsistencyFixType.UPSERT)
                  .description(
                      String.format(
                          "Assertion missing entityUrn - derived from type-specific properties: %s",
                          derivedEntityUrn))
                  .batchItems(List.of(upsertItem))
                  .build());
        }
      } catch (Exception e) {
        log.debug(
            "Could not derive entityUrn for assertion {} from type-specific properties: {}",
            assertionUrn,
            e.getMessage());
      }

      // Cannot derive entityUrn - soft delete
      return List.of(
          createSoftDeleteIssue(
              assertionUrn,
              "Assertion has no entityUrn and cannot derive from type-specific properties",
              List.of(createSoftDeleteItem(ctx, assertionUrn, response))));
    }

    return Collections.emptyList();
  }
}
