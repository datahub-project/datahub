package datahub.client.patch.tests;

import com.linkedin.test.TestResult;
import com.linkedin.test.TestResults;
import datahub.client.patch.AbstractMultiFieldPatchBuilder;
import datahub.client.patch.PatchOperationType;
import org.apache.commons.lang3.tuple.ImmutableTriple;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static com.fasterxml.jackson.databind.node.JsonNodeFactory.instance;
import static com.linkedin.metadata.Constants.TEST_RESULTS_ASPECT_NAME;


public class TestResultsPatchBuilder extends AbstractMultiFieldPatchBuilder<TestResultsPatchBuilder> {

  private static final String PASSING_BASE_PATH = "/passing/";
  private static final String FAILING_BASE_PATH = "/failing/";
  private static final String TEST_KEY = "test";
  private static final String TYPE_KEY = "type";

  /**
   * Add test results in passing/failing arrays
   * @param testResults test results to add
   * @return builder
   */
  public TestResultsPatchBuilder updateTestResults(@Nonnull TestResults testResults) {
    // tests can be passing or failing but not both so add/remove
    for (TestResult passing : testResults.getPassing()) {
      addPatchOperation(PatchOperationType.ADD, PASSING_BASE_PATH + passing.getTest(), passing);
      addPatchOperation(PatchOperationType.REMOVE, FAILING_BASE_PATH  + passing.getTest(), null);
    }

    for (TestResult failing : testResults.getFailing()) {
      addPatchOperation(PatchOperationType.ADD, FAILING_BASE_PATH + failing.getTest(), failing);
      addPatchOperation(PatchOperationType.REMOVE, PASSING_BASE_PATH  + failing.getTest(), null);
    }

    return this;
  }

  private void addPatchOperation(PatchOperationType op, String path, @Nullable TestResult testResult) {
    pathValues.add(ImmutableTriple.of(op.getValue(), path,
            testResult != null ? instance.objectNode()
                    .put(TEST_KEY, testResult.getTest().toString())
                    .put(TYPE_KEY, testResult.getType().toString()) : null));
  }

  @Override
  protected String getAspectName() {
    return TEST_RESULTS_ASPECT_NAME;
  }

  @Override
  protected String getEntityType() {
    if (this.targetEntityUrn == null) {
      throw new IllegalStateException("Target Entity Urn must be set to determine entity type before building Patch.");
    }
    return this.targetEntityUrn.getEntityType();
  }
}
