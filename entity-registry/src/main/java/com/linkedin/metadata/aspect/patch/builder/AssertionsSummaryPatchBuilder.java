package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.common.AssertionSummaryDetails;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import javax.annotation.Nonnull;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.tuple.ImmutableTriple;

@ToString
@EqualsAndHashCode
public class AssertionsSummaryPatchBuilder
    extends AbstractMultiFieldPatchBuilder<AssertionsSummaryPatchBuilder> {

  private static final String LEGACY_PASSING_ASSERTIONS_START = "/passingAssertions/";
  private static final String PASSING_ASSERTION_DETAILS_START = "/passingAssertionDetails/";
  private static final String LEGACY_FAILING_ASSERTIONS_START = "/failingAssertions/";
  private static final String FAILING_ASSERTION_DETAILS_START = "/failingAssertionDetails/";
  private static final String ASSERTION_URN_KEY = "urn";
  private static final String TYPE_KEY = "type";
  private static final String LAST_RESULT_AT_KEY = "lastResultAt";
  private static final String SOURCE_KEY = "source";

  private String entityName;

  public AssertionsSummaryPatchBuilder addPassingAssertionDetails(
      @Nonnull final AssertionSummaryDetails details) {
    ObjectNode value = instance.objectNode();
    value
        .put(ASSERTION_URN_KEY, details.getUrn().toString())
        .put(TYPE_KEY, details.getType())
        .put(LAST_RESULT_AT_KEY, details.getLastResultAt());

    if (details.hasSource()) {
      value.put(SOURCE_KEY, details.getSource());
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            PASSING_ASSERTION_DETAILS_START + encodeValueUrn(details.getUrn()),
            value));
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            LEGACY_PASSING_ASSERTIONS_START + encodeValueUrn(details.getUrn()),
            null));
    return this;
  }

  public AssertionsSummaryPatchBuilder removeFromPassingAssertionDetails(
      @Nonnull final Urn assertionUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            PASSING_ASSERTION_DETAILS_START + encodeValueUrn(assertionUrn),
            null));
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            LEGACY_PASSING_ASSERTIONS_START + encodeValueUrn(assertionUrn),
            null));
    return this;
  }

  public AssertionsSummaryPatchBuilder addFailingAssertionDetails(
      @Nonnull final AssertionSummaryDetails details) {
    ObjectNode value = instance.objectNode();
    value
        .put(ASSERTION_URN_KEY, details.getUrn().toString())
        .put(TYPE_KEY, details.getType())
        .put(LAST_RESULT_AT_KEY, details.getLastResultAt());

    if (details.hasSource()) {
      value.put(SOURCE_KEY, details.getSource());
    }

    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.ADD.getValue(),
            FAILING_ASSERTION_DETAILS_START + encodeValueUrn(details.getUrn()),
            value));
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            LEGACY_FAILING_ASSERTIONS_START + encodeValueUrn(details.getUrn()),
            null));
    return this;
  }

  public AssertionsSummaryPatchBuilder removeFromFailingAssertionDetails(
      @Nonnull final Urn assertionUrn) {
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            FAILING_ASSERTION_DETAILS_START + encodeValueUrn(assertionUrn),
            null));
    pathValues.add(
        ImmutableTriple.of(
            PatchOperationType.REMOVE.getValue(),
            LEGACY_FAILING_ASSERTIONS_START + encodeValueUrn(assertionUrn),
            null));
    return this;
  }

  public AssertionsSummaryPatchBuilder withEntityName(@Nonnull final String entityName) {
    this.entityName = entityName;
    return this;
  }

  @Override
  protected String getAspectName() {
    return Constants.ASSERTIONS_SUMMARY_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    return this.entityName;
  }
}
