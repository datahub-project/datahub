package com.linkedin.metadata.config.productupdate;

import java.util.Optional;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ReleaseVersionTest {

  @Test
  public void testParsesTheSpellingsUsedAcrossTheRepo() {
    Assert.assertEquals(parse("v1.7.0").toString(), "v1.7.0");
    Assert.assertEquals(parse("1.7.0").toString(), "v1.7.0");
    Assert.assertEquals(parse("v_2_1_0").toString(), "v2.1.0");
    Assert.assertEquals(parse("v1.5.0.7").toString(), "v1.5.0.7");
  }

  @Test
  public void testRejectsNonVersions() {
    Assert.assertTrue(ReleaseVersion.parse("Next").isEmpty());
    Assert.assertTrue(ReleaseVersion.parse("<version number>").isEmpty());
    Assert.assertTrue(ReleaseVersion.parse("v1.7.0-rc1").isEmpty());
    Assert.assertTrue(ReleaseVersion.parse(null).isEmpty());
  }

  @Test
  public void testSameMinorSeriesSpansPrecisionAndPatches() {
    // Cloud declares v2.1 for the release its notes call v2.1.0.
    Assert.assertTrue(parse("v2.1").sameMinorSeries(parse("v2.1.0")));
    // A hotfix rollup doesn't get its own toast.
    Assert.assertTrue(parse("v1.7.0").sameMinorSeries(parse("v1.7.0.3")));
    // A missed minor release does.
    Assert.assertFalse(parse("v1.4.0").sameMinorSeries(parse("v1.7.0")));
    Assert.assertFalse(parse("v2.1").sameMinorSeries(parse("v3.1")));
  }

  @Test
  public void testOrdersByComponentNotLexically() {
    Assert.assertTrue(parse("v1.7.0").compareTo(parse("v1.10.0")) < 0);
    Assert.assertTrue(parse("v1.5.0.7").compareTo(parse("v1.5.0")) > 0);
    Assert.assertEquals(parse("v2.1"), parse("v2.1.0"));
  }

  @Test
  public void testAppearsInAcceptsAnySeparatorAtDigitBoundaries() {
    Assert.assertTrue(parse("v1.7.0").appearsIn("https://docs.datahub.com/docs/releases#v1-7-0"));
    Assert.assertTrue(parse("v2.1").appearsIn("https://datahub.com/blog/datahub-cloud-2-1"));
    Assert.assertTrue(parse("v1.7.0").appearsIn("Explore version v1.7.0"));
    Assert.assertFalse(parse("v2.1").appearsIn("https://datahub.com/blog/datahub-cloud-2-10"));
    Assert.assertFalse(parse("v1.4.0").appearsIn("https://docs.datahub.com/docs/releases#v1-7-0"));
  }

  @Test
  public void testHighestInFreeText() {
    Assert.assertEquals(
        ReleaseVersion.highestIn("Explore version v1.7.0"), Optional.of(parse("v1.7.0")));
    Assert.assertTrue(ReleaseVersion.highestIn("Business meaning meets your agents").isEmpty());
  }

  private static ReleaseVersion parse(String raw) {
    Optional<ReleaseVersion> parsed = ReleaseVersion.parse(raw);
    Assert.assertTrue(parsed.isPresent(), "Expected to parse " + raw);
    return parsed.get();
  }
}
