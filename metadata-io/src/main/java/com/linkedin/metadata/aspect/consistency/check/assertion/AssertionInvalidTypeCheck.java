package com.linkedin.metadata.aspect.consistency.check.assertion;

import static com.linkedin.metadata.Constants.ASSERTION_ENTITY_NAME;
import static com.linkedin.metadata.Constants.ASSERTION_INFO_ASPECT_NAME;
import static com.linkedin.metadata.Constants.STATUS_ASPECT_NAME;

import com.linkedin.metadata.aspect.consistency.check.InvalidEnumCheck;
import com.linkedin.metadata.aspect.consistency.fix.ConsistencyFixType;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Consistency check that detects assertions with invalid/unknown type enum values.
 *
 * <p>This check extends {@link InvalidEnumCheck} to specifically target the {@code type} field in
 * {@code AssertionInfo} aspect. Invalid enum values can occur when:
 *
 * <ul>
 *   <li>Schema evolution removed assertion types that still exist in data (e.g., "SLA")
 *   <li>External systems wrote invalid assertion type values directly to storage
 * </ul>
 *
 * <p><b>Example Error:</b>
 *
 * <pre>{@code
 * ERROR :: /type :: "SLA" is not an enum symbol
 * }</pre>
 *
 * <p><b>Why HARD_DELETE:</b> Assertions with invalid type enum values cannot be deserialized into
 * valid {@code AssertionInfo} objects. The {@code AbstractAssertionCheck.getAssertionInfo()} method
 * would throw an exception trying to construct the record, and other checks that depend on valid
 * AssertionInfo would fail. These assertions are effectively corrupted and should be removed.
 *
 * <p><b>Enabled by Default:</b> Unlike the base {@link InvalidEnumCheck}, this check runs by
 * default for assertion entities since invalid assertion types are a known data quality issue.
 */
@Slf4j
@Component("consistencyAssertionInvalidTypeCheck")
public class AssertionInvalidTypeCheck extends InvalidEnumCheck {

  @Override
  public boolean isOnDemandOnly() {
    return false; // Enabled by default for assertions
  }

  @Override
  @Nonnull
  protected ConsistencyFixType getFixType() {
    return ConsistencyFixType.HARD_DELETE; // Delete entire entity for assertions
  }

  @Override
  @Nonnull
  public String getEntityType() {
    return ASSERTION_ENTITY_NAME;
  }

  @Override
  @Nonnull
  public String getName() {
    return "Assertion Invalid Type";
  }

  @Override
  @Nonnull
  public String getDescription() {
    return "Detects assertions with invalid/unknown type enum values";
  }

  @Override
  @Nonnull
  public Set<String> getTargetAspects() {
    return Set.of(ASSERTION_INFO_ASPECT_NAME);
  }

  @Override
  @Nonnull
  public Optional<Set<String>> getRequiredAspects() {
    return Optional.of(Set.of(ASSERTION_INFO_ASPECT_NAME, STATUS_ASPECT_NAME));
  }
}
