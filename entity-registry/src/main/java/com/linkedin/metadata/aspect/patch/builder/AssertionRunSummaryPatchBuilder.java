package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutableTriple;

@ToString
@EqualsAndHashCode(callSuper = false)
public class AssertionRunSummaryPatchBuilder
    extends AbstractMultiFieldPatchBuilder<AssertionRunSummaryPatchBuilder> {

  private static final String LAST_PASSED_AT_PATH = "/lastPassedAtMillis";
  private static final String LAST_ERRORED_AT_PATH = "/lastErroredAtMillis";
  private static final String LAST_FAILED_AT_PATH = "/lastFailedAtMillis";

  public AssertionRunSummaryPatchBuilder setLastPassedAt(@Nonnull final Long timestamp) {
    ObjectNode value = instance.objectNode();

    // Explicitly create a number node for the timestamp
    value.put(LAST_PASSED_AT_PATH, timestamp); // `put()` automatically handles numeric types

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            LAST_PASSED_AT_PATH,
            value.get(LAST_PASSED_AT_PATH)));

    return this;
  }

  public AssertionRunSummaryPatchBuilder setLastFailedAt(@Nonnull final Long timestamp) {
    ObjectNode value = instance.objectNode();

    // Explicitly create a number node for the timestamp
    value.put(LAST_FAILED_AT_PATH, timestamp); // `put()` automatically handles numeric types

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            LAST_FAILED_AT_PATH,
            value.get(LAST_FAILED_AT_PATH)));

    return this;
  }

  public AssertionRunSummaryPatchBuilder setLastErroredAt(@Nonnull final Long timestamp) {
    ObjectNode value = instance.objectNode();

    // Explicitly create a number node for the timestamp
    value.put(LAST_ERRORED_AT_PATH, timestamp); // `put()` automatically handles numeric types

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            LAST_ERRORED_AT_PATH,
            value.get(LAST_ERRORED_AT_PATH)));

    return this;
  }

  @Override
  protected String getAspectName() {
    return Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return Constants.ASSERTION_ENTITY_NAME;
  }
}
