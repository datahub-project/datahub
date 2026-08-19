package com.linkedin.metadata.entity.retention.buffer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertThrows;

import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.SimpleRetentionKey;
import org.testng.annotations.Test;

public class RetentionKeyTest {

  private static final String URN =
      "urn:li:dataset:(urn:li:dataPlatform:mysql,my_db.my_table,PROD)";
  private static final String ASPECT = "status";

  @Test
  public void testEqualsAndHashCodeForSameUrnAndAspect() {
    RetentionKey a = new SimpleRetentionKey(URN, ASPECT);
    RetentionKey b = new SimpleRetentionKey(URN, ASPECT);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  @Test
  public void testNotEqualWhenUrnDiffers() {
    RetentionKey a = new SimpleRetentionKey(URN, ASPECT);
    RetentionKey b =
        new SimpleRetentionKey("urn:li:dataset:(urn:li:dataPlatform:mysql,other,PROD)", ASPECT);

    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualWhenAspectDiffers() {
    RetentionKey a = new SimpleRetentionKey(URN, ASPECT);
    RetentionKey b = new SimpleRetentionKey(URN, "ownership");

    assertNotEquals(a, b);
  }

  @Test
  public void testNotEqualToNullOrOtherType() {
    RetentionKey a = new SimpleRetentionKey(URN, ASPECT);

    assertNotEquals(a, null);
    assertNotEquals(a, "not a RetentionKey");
  }

  @Test
  public void testConstructorRejectsNulls() {
    assertThrows(NullPointerException.class, () -> new SimpleRetentionKey(null, ASPECT));
    assertThrows(NullPointerException.class, () -> new SimpleRetentionKey(URN, null));
  }
}
