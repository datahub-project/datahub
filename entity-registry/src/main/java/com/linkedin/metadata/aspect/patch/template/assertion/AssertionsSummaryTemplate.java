package com.linkedin.metadata.aspect.patch.template.assertion;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AssertionSummaryDetailsArray;
import com.linkedin.common.AssertionsSummary;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.CompoundKeyTemplate;
import java.util.Collections;
import javax.annotation.Nonnull;

/**
 * Patch template for {@link AssertionsSummary} aspect. Allows adding and removing assertion summary
 * details atomically.
 */
public class AssertionsSummaryTemplate extends CompoundKeyTemplate<AssertionsSummary> {
  private static final String LEGACY_PASSING_ASSERTIONS_FIELD_NAME = "passingAssertions";
  private static final String PASSING_ASSERTION_DETAILS_FIELD_NAME = "passingAssertionDetails";
  private static final String LEGACY_FAILING_ASSERTIONS_FIELD_NAME = "failingAssertions";
  private static final String FAILING_ASSERTION_DETAILS_FIELD_NAME = "failingAssertionDetails";
  private static final String ERRORING_ASSERTION_DETAILS_FIELD_NAME = "erroringAssertionDetails";
  private static final String URN_FIELD_NAME = "urn";

  @Override
  public AssertionsSummary getSubtype(RecordTemplate recordTemplate) throws ClassCastException {
    if (recordTemplate instanceof AssertionsSummary) {
      return (AssertionsSummary) recordTemplate;
    }
    throw new ClassCastException("Unable to cast RecordTemplate to AssertionsSummary");
  }

  @Override
  public Class<AssertionsSummary> getTemplateType() {
    return AssertionsSummary.class;
  }

  @Nonnull
  @Override
  public AssertionsSummary getDefault() {
    AssertionsSummary assertionsSummary = new AssertionsSummary();
    assertionsSummary.setPassingAssertionDetails(new AssertionSummaryDetailsArray());
    assertionsSummary.setFailingAssertionDetails(new AssertionSummaryDetailsArray());
    assertionsSummary.setErroringAssertionDetails(new AssertionSummaryDetailsArray());
    return assertionsSummary;
  }

  @Nonnull
  @Override
  public JsonNode transformFields(JsonNode baseNode) {
    JsonNode transformedNode =
        arrayFieldToMap(
            baseNode,
            PASSING_ASSERTION_DETAILS_FIELD_NAME,
            Collections.singletonList(URN_FIELD_NAME));
    transformedNode =
        arrayFieldToMap(
            transformedNode, LEGACY_PASSING_ASSERTIONS_FIELD_NAME, Collections.emptyList());
    transformedNode =
        arrayFieldToMap(
            transformedNode,
            FAILING_ASSERTION_DETAILS_FIELD_NAME,
            Collections.singletonList(URN_FIELD_NAME));
    transformedNode =
        arrayFieldToMap(
            transformedNode, LEGACY_FAILING_ASSERTIONS_FIELD_NAME, Collections.emptyList());
    transformedNode =
        arrayFieldToMap(
            transformedNode,
            ERRORING_ASSERTION_DETAILS_FIELD_NAME,
            Collections.singletonList(URN_FIELD_NAME));
    return transformedNode;
  }

  @Nonnull
  @Override
  public JsonNode rebaseFields(JsonNode patched) {
    JsonNode rebasedNode =
        transformedMapToArray(
            patched,
            PASSING_ASSERTION_DETAILS_FIELD_NAME,
            Collections.singletonList(URN_FIELD_NAME));
    rebasedNode =
        transformedMapToArray(
            rebasedNode, LEGACY_PASSING_ASSERTIONS_FIELD_NAME, Collections.emptyList());
    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            FAILING_ASSERTION_DETAILS_FIELD_NAME,
            Collections.singletonList(URN_FIELD_NAME));
    rebasedNode =
        transformedMapToArray(
            rebasedNode, LEGACY_FAILING_ASSERTIONS_FIELD_NAME, Collections.emptyList());
    rebasedNode =
        transformedMapToArray(
            rebasedNode,
            ERRORING_ASSERTION_DETAILS_FIELD_NAME,
            Collections.singletonList(URN_FIELD_NAME));
    return rebasedNode;
  }
}
