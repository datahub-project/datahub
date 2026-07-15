package com.linkedin.metadata.graph.postgres;

import java.nio.charset.StandardCharsets;
import org.testng.Assert;
import org.testng.annotations.Test;

public class XxHash64Test {

  /** Reference: XXH64("", 0) per xxHash specification. */
  private static final long EMPTY_SEED0 = 0xEF46DB3751D8E999L;

  @Test
  public void emptyInputSeedZeroMatchesReference() {
    Assert.assertEquals(XxHash64.hashBytes(new byte[0], 0L), EMPTY_SEED0);
    Assert.assertEquals(XxHash64.hashUtf8(""), EMPTY_SEED0);
  }

  @Test
  public void utf8StableAcrossCalls() {
    String u = "urn:li:dataset:(urn:li:dataPlatform:hive,foo,PROD)";
    Assert.assertEquals(XxHash64.hashUtf8(u), XxHash64.hashUtf8(u));
    Assert.assertEquals(
        XxHash64.hashUtf8(u), XxHash64.hashBytes(u.getBytes(StandardCharsets.UTF_8), 0L));
  }
}
