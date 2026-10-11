package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.*;

import com.linkedin.assertion.AssertionInfo;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.metadata.aspect.consistency.ConsistencyIssue;
import com.linkedin.metadata.aspect.consistency.check.AbstractEntityCheck;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.models.registry.EntityRegistry;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * Abstract base class for assertion consistency checks.
 *
 * <p>Provides common functionality for assertion checks:
 *
 * <ul>
 *   <li>Default required aspects for assertion entity type
 *   <li>Extraction of AssertionInfo from entity response
 *   <li>Validation of entity types valid for assertions (derived from PDL annotations)
 *   <li>Caching of assertion info in context for other checks to use
 * </ul>
 *
 * <p>Subclasses implement {@link #checkAssertion} to perform the specific check logic on assertions
 * with valid AssertionInfo.
 */
@Slf4j
public abstract class AbstractAssertionCheck extends AbstractEntityCheck {

  /** Default aspects required by assertion checks */
  protected static final Set<String> DEFAULT_ASSERTION_ASPECTS =
      Set.of(ASSERTION_INFO_ASPECT_NAME, STATUS_ASPECT_NAME);

  /** Valid entity types for assertion primary entities (computed once from entity registry) */
  @Getter private final Set<String> validAssertionEntityTypes;

  /**
   * Construct with entity registry to derive valid assertion entity types.
   *
   * @param entityRegistry the entity registry for deriving valid entity types from PDL annotations
   */
  protected AbstractAssertionCheck(@Nonnull EntityRegistry entityRegistry) {
    // Exclude schemaField as it's only valid for secondary targets, not primary entities
    this.validAssertionEntityTypes =
        Collections.unmodifiableSet(
            computeValidEntityTypesFromAspect(
                entityRegistry,
                ASSERTION_ENTITY_NAME,
                ASSERTION_INFO_ASPECT_NAME,
                Set.of(SCHEMA_FIELD_ENTITY_NAME)));
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ASSERTION_ENTITY_NAME;
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    return Optional.of(DEFAULT_ASSERTION_ASPECTS);
  }

  @Override
  @Nonnull
  protected final List<ConsistencyIssue> checkEntity(
      @Nonnull CheckContext ctx, @Nonnull Urn urn, @Nonnull EntityResponse response) {

    // Extract AssertionInfo
    AssertionInfo assertionInfo = getAssertionInfo(response);
    if (assertionInfo == null) {
      return Collections.emptyList();
    }

    // Cache the assertion info in context for other checks to use
    ctx.cacheAspect(urn, ASSERTION_INFO_ASPECT_NAME, assertionInfo);

    return checkAssertion(ctx, urn, response, assertionInfo);
  }

  /**
   * Perform the specific consistency check on an assertion.
   *
   * @param ctx check context
   * @param assertionUrn URN of the assertion
   * @param response entity response
   * @param assertionInfo the assertion info aspect
   * @return list of issues found
   */
  @Nonnull
  protected abstract List<ConsistencyIssue> checkAssertion(
      @Nonnull CheckContext ctx,
      @Nonnull Urn assertionUrn,
      @Nonnull EntityResponse response,
      @Nonnull AssertionInfo assertionInfo);

  /**
   * Extract AssertionInfo from the response.
   *
   * @param response entity response
   * @return AssertionInfo or null if not present
   */
  @Nullable
  protected AssertionInfo getAssertionInfo(@Nonnull EntityResponse response) {
    EnvelopedAspect infoAspect = getAspect(response, ASSERTION_INFO_ASPECT_NAME);
    if (infoAspect == null) {
      return null;
    }
    return new AssertionInfo(infoAspect.getValue().data());
  }

  /**
   * Check if the entity type is valid for the primary entity of an assertion.
   *
   * <p>Valid entity types are derived from @UrnValidation annotations in the assertionInfo aspect
   * PDL model at construction time.
   *
   * @param entityType entity type string to validate
   * @return true if valid
   */
  protected boolean isValidAssertionEntityType(String entityType) {
    return validAssertionEntityTypes.contains(entityType);
  }
}
