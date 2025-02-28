package io.datahubproject.openapi.scim.repositories;

import io.restassured.response.ValidatableResponse;
import java.time.LocalDateTime;
import java.time.ZoneId;
import org.apache.directory.scim.compliance.tests.ScimpleITSupport;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

public class ScimRepositoryTestBase extends ScimpleITSupport {
  static String CREATED = "meta.created";
  static String LAST_MODIFIED = "meta.lastModified";

  private static int LATENCY_THRESHOLD_MS = 10000;

  private static int SYS_THRESHOLD_MS = 1000;

  private static long ts(String s) {
    return LocalDateTime.parse(s).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
  }

  private static class TimestampMatcher extends BaseMatcher<String> {

    private final long lesserBaseline;
    private final int threshold;

    private TimestampMatcher(long lesserBaseline, int threshold) {
      this.lesserBaseline = lesserBaseline;
      this.threshold = threshold;
    }

    @Override
    public boolean matches(Object o) {
      if (!(o instanceof String)) {
        return false;
      }
      String s = (String) o;
      if (s == null || s.isEmpty()) {
        return false;
      }
      long greaterValue = ts(s);
      return lesserBaseline <= greaterValue && (lesserBaseline + threshold) >= greaterValue;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText(
          " to be within "
              + threshold
              + " msecs of "
              + AbstractScimRepository.metaTimestamp(lesserBaseline));
    }
  }

  Matcher<String> isTsSys(long lesserBaseline) {
    return new TimestampMatcher(lesserBaseline, SYS_THRESHOLD_MS);
  }

  Matcher<String> isTsLatency(long lesserBaseline) {
    return new TimestampMatcher(lesserBaseline, LATENCY_THRESHOLD_MS);
  }

  Matcher<String> isTs(long lesserBaseline, int threshold) {
    return new TimestampMatcher(lesserBaseline, threshold);
  }

  static long lastModifiedTs(ValidatableResponse validatableResponse) {
    String modified = validatableResponse.extract().body().path(LAST_MODIFIED);

    return ts(modified);
  }
}
