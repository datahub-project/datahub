package com.linkedin.metadata.aspect.patch.builder;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.TEST_RESULTS_ASPECT_NAME;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.patch.PatchOperationType;
import com.linkedin.test.TestResult;
import com.linkedin.test.TestResults;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.ImmutableTriple;

public class TestResultsPatchBuilder
    extends AbstractMultiFieldPatchBuilder<TestResultsPatchBuilder> {

  private static final String PASSING_BASE_PATH = "/passing/";
  private static final String FAILING_BASE_PATH = "/failing/";
  private static final String TEST_KEY = "test";
  private static final String TYPE_KEY = "type";

  /**
   * Add test results in passing/failing arrays
   *
   * @param testResults test results to add
   * @return builder
   */
  public TestResultsPatchBuilder updateTestResults(@Nonnull TestResults testResults) {
    // tests can be passing or failing but not both so add/remove
    for (TestResult passing : testResults.getPassing()) {
      addPatchOperation(PatchOperationType.ADD, PASSING_BASE_PATH + passing.getTest(), passing);
      addPatchOperation(PatchOperationType.REMOVE, FAILING_BASE_PATH + passing.getTest(), null);
    }

    for (TestResult failing : testResults.getFailing()) {
      addPatchOperation(PatchOperationType.ADD, FAILING_BASE_PATH + failing.getTest(), failing);
      addPatchOperation(PatchOperationType.REMOVE, PASSING_BASE_PATH + failing.getTest(), null);
    }

    return this;
  }

  public TestResultsPatchBuilder addPassing(@Nonnull Urn testUrn, @Nonnull TestResult testResult) {
    addPatchOperation(PatchOperationType.ADD, PASSING_BASE_PATH + testUrn.toString(), testResult);
    return this;
  }

  public TestResultsPatchBuilder addFailing(@Nonnull Urn testUrn, @Nonnull TestResult testResult) {
    addPatchOperation(PatchOperationType.ADD, FAILING_BASE_PATH + testUrn.toString(), testResult);
    return this;
  }

  public TestResultsPatchBuilder removePassing(
      @Nonnull Urn testUrn, @Nonnull TestResult testResult) {
    addPatchOperation(
        PatchOperationType.REMOVE, PASSING_BASE_PATH + testUrn.toString(), testResult);
    return this;
  }

  public TestResultsPatchBuilder removeFailing(
      @Nonnull Urn testUrn, @Nonnull TestResult testResult) {
    addPatchOperation(
        PatchOperationType.REMOVE, FAILING_BASE_PATH + testUrn.toString(), testResult);
    return this;
  }

  private void addPatchOperation(
      PatchOperationType op, String path, @Nullable TestResult testResult) {
    pathValues.add(
        ImmutableTriple.of(
            op.getValue(),
            path,
            testResult != null
                ? instance
                    .objectNode()
                    .put(TEST_KEY, testResult.getTest().toString())
                    .put(TYPE_KEY, testResult.getType().toString())
                    .put(
                        "testDefinitionMd5",
                        testResult.getTestDefinitionMd5() != null
                            ? testResult.getTestDefinitionMd5()
                            : null)
                : null));
  }

  @Override
  protected String getAspectName() {
    return TEST_RESULTS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException(
          "Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }

  public boolean hasPatchOperations() {
    return !pathValues.isEmpty();
  }
}
