package com.linkedin.metadata.entity.retention.buffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class RetentionKeyTest {

  private static final String URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)";
  private static final String ASPECT = "status";

  @Test
  public void testEqualsAndHashCodeForSameUrnAndAspect() {
    RetentionKey a = new RetentionKey(URN, ASPECT);
    RetentionKey b = new RetentionKey(URN, ASPECT);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testNotEqualWhenUrnDiffers() {
    RetentionKey a = new RetentionKey(URN, ASPECT);
    RetentionKey b =
        new RetentionKey("urn:li:dataset:(urn:li:dataPlatform:mysql,other,PROD)", ASPECT);

    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualWhenAspectDiffers() {
    RetentionKey a = new RetentionKey(URN, ASPECT);
    RetentionKey b = new RetentionKey(URN, "ownership");

    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualToNullOrOtherType() {
    RetentionKey a = new RetentionKey(URN, ASPECT);

    assertNotEquals(a, null);
    assertNotEquals(a, "not a RetentionKey");
  }

  @Test
  public void testConstructorRejectsNulls() {
    assertThrows(NullPointerException.class, () -> new RetentionKey(null, ASPECT));
    assertThrows(NullPointerException.class, () -> new RetentionKey(URN, null));
  }
}
