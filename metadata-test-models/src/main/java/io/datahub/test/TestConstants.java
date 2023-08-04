package io.datahub.test;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.key.TestKey;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.test.TestResultArray;
import com.linkedin.test.TestResults;


public class TestConstants {
  public static final TestResults EMPTY_RESULTS = new TestResults().setPassing(new TestResultArray()).setFailing(new TestResultArray());
  public static final Urn DUMMY_TEST_URN = EntityKeyUtils.convertEntityKeyToUrn(new TestKey().setId("dummy"), Constants.TEST_ENTITY_NAME);

  private TestConstants() {
  }
}
