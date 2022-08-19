package datahub.spark.model.dataset;

import java.util.Arrays;
import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.testng.Assert;

@RunWith(Parameterized.class)
public class TestS3UriMatching {

  private String pathSpec;
  private String matchedUri;
  private static final String PATH = "s3://my-bucket/foo/tests/bar.avro";

  public TestS3UriMatching(String pathSpec, String matchedUri) {
    super();
    this.pathSpec = pathSpec;
    this.matchedUri = matchedUri;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        
        { "s3://my-bucket/foo/tests/{table}", "s3://my-bucket/foo/tests/bar.avro" },
        { "s3://my-bucket/{table}/*/*", "s3://my-bucket/foo" },
        { "s3://my-bucket/{table}", "s3://my-bucket/foo" },
        { "s3://my-bucket/*/*/{table}", "s3://my-bucket/foo/tests/bar.avro" },
        { "s3://my-bucket/*/foo/", null },
        { "s3://my-bucket/foo/*/*/*/{table}", null },
        { "s3://my-bucket/foo/{table}/*", "s3://my-bucket/foo/tests" }
    });
  }

  @Test
  public void testS3UriMatching() {
    Assert.assertEquals(matchedUri, HdfsPathDataset.getMatchedUri(PATH, pathSpec));
  }
}
