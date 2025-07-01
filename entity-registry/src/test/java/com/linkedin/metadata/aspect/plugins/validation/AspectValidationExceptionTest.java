package com.linkedin.metadata.aspect.plugins.validation;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.aspect.batch.BatchItem;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.mxe.SystemMetadata;
import com.linkedin.util.Pair;
import java.net.URISyntaxException;
import lombok.Getter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AspectValidationExceptionTest {

  /** Test implementation of BatchItem for unit testing */
  @Getter
  private static class TestBatchItem implements BatchItem {
    private final Urn urn;
    private final String aspectName;
    private final ChangeType changeType;
    private final AuditStamp auditStamp;
    private final RecordTemplate recordTemplate;
    private final EntitySpec entitySpec;
    private final AspectSpec aspectSpec;
    private final SystemMetadata systemMetadata;

    public TestBatchItem(Urn urn, String aspectName, ChangeType changeType, AuditStamp auditStamp) {
      this.urn = urn;
      this.aspectName = aspectName;
      this.changeType = changeType;
      this.auditStamp = auditStamp;
      this.recordTemplate = null;
      this.entitySpec = null;
      this.aspectSpec = null;
      this.systemMetadata = null;
    }

    @Override
    public boolean isDatabaseDuplicateOf(BatchItem other) {
      if (this == other) return true;
      if (other == null) return false;
      return urn.equals(other.getUrn())
          && aspectName.equals(other.getAspectName())
          && changeType.equals(other.getChangeType());
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TestBatchItem that = (TestBatchItem) o;
      return urn.equals(that.urn)
          && aspectName.equals(that.aspectName)
          && changeType.equals(that.changeType);
    }

    @Override
    public int hashCode() {
      return urn.hashCode() + aspectName.hashCode() + changeType.hashCode();
    }
  }

  private BatchItem testItem;
  private Urn testUrn;
  private AuditStamp testAuditStamp;
  private final String TEST_ASPECT_NAME = "testAspect";
  private final String TEST_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:kafka,test-dataset,PROD)";

  @BeforeMethod
  public void setup() throws URISyntaxException {
    // Setup URN
    testUrn = Urn.createFromString(TEST_URN_STRING);

    // Setup AuditStamp
    testAuditStamp = new AuditStamp();
    testAuditStamp.setTime(System.currentTimeMillis());
    testAuditStamp.setActor(Urn.createFromString("urn:li:corpuser:testUser"));

    // Create test item
    testItem = createTestBatchItem();
  }

  private BatchItem createTestBatchItem() {
    return new TestBatchItem(testUrn, TEST_ASPECT_NAME, ChangeType.UPSERT, testAuditStamp);
  }

  private BatchItem createTestBatchItem(ChangeType changeType) {
    return new TestBatchItem(testUrn, TEST_ASPECT_NAME, changeType, testAuditStamp);
  }

  @Test
  public void testForItem() {
    String errorMsg = "Validation failed";
    AspectValidationException exception = AspectValidationException.forItem(testItem, errorMsg);

    assertNotNull(exception);
    assertEquals(exception.getItem(), testItem);
    assertEquals(exception.getMsg(), errorMsg);
    assertEquals(exception.getMessage(), errorMsg);
    assertEquals(exception.getSubType(), ValidationSubType.VALIDATION);
    assertEquals(exception.getChangeType(), ChangeType.UPSERT);
    assertEquals(exception.getEntityUrn(), testUrn);
    assertEquals(exception.getAspectName(), TEST_ASPECT_NAME);
    assertNull(exception.getCause());
  }

  @Test
  public void testForItemWithCause() {
    String errorMsg = "Validation failed with cause";
    Exception cause = new RuntimeException("Root cause");
    AspectValidationException exception =
        AspectValidationException.forItem(testItem, errorMsg, cause);

    assertNotNull(exception);
    assertEquals(exception.getItem(), testItem);
    assertEquals(exception.getMsg(), errorMsg);
    assertEquals(exception.getMessage(), errorMsg);
    assertEquals(exception.getSubType(), ValidationSubType.VALIDATION);
    assertEquals(exception.getCause(), cause);
  }

  @Test
  public void testForPrecondition() {
    String errorMsg = "Precondition failed";
    AspectValidationException exception =
        AspectValidationException.forPrecondition(testItem, errorMsg);

    assertNotNull(exception);
    assertEquals(exception.getItem(), testItem);
    assertEquals(exception.getMsg(), errorMsg);
    assertEquals(exception.getSubType(), ValidationSubType.PRECONDITION);
    assertNull(exception.getCause());
  }

  @Test
  public void testForPreconditionWithCause() {
    String errorMsg = "Precondition failed with cause";
    Exception cause = new IllegalStateException("Precondition violation");
    AspectValidationException exception =
        AspectValidationException.forPrecondition(testItem, errorMsg, cause);

    assertNotNull(exception);
    assertEquals(exception.getItem(), testItem);
    assertEquals(exception.getMsg(), errorMsg);
    assertEquals(exception.getSubType(), ValidationSubType.PRECONDITION);
    assertEquals(exception.getCause(), cause);
  }

  @Test
  public void testForFilter() {
    String errorMsg = "Filter validation failed";
    AspectValidationException exception = AspectValidationException.forFilter(testItem, errorMsg);

    assertNotNull(exception);
    assertEquals(exception.getItem(), testItem);
    assertEquals(exception.getMsg(), errorMsg);
    assertEquals(exception.getSubType(), ValidationSubType.FILTER);
    assertNull(exception.getCause());
  }

  @Test
  public void testForAuth() {
    String errorMsg = "Authorization failed";
    AspectValidationException exception = AspectValidationException.forAuth(testItem, errorMsg);

    assertNotNull(exception);
    assertEquals(exception.getItem(), testItem);
    assertEquals(exception.getMsg(), errorMsg);
    assertEquals(exception.getSubType(), ValidationSubType.AUTHORIZATION);
    assertNull(exception.getCause());
  }

  @Test
  public void testGetAspectGroup() {
    AspectValidationException exception = AspectValidationException.forItem(testItem, "test");
    Pair<Urn, String> aspectGroup = exception.getAspectGroup();

    assertNotNull(aspectGroup);
    assertEquals(aspectGroup.getFirst(), testUrn);
    assertEquals(aspectGroup.getSecond(), TEST_ASPECT_NAME);
  }

  @Test
  public void testConstructorWithNullSubType() {
    AspectValidationException exception =
        new AspectValidationException(testItem, "test msg", null, null);

    // Should default to VALIDATION when null
    assertEquals(exception.getSubType(), ValidationSubType.VALIDATION);
  }

  @Test
  public void testEqualsAndHashCode() {
    String msg = "Same message";
    AspectValidationException exception1 = AspectValidationException.forItem(testItem, msg);
    AspectValidationException exception2 = AspectValidationException.forItem(testItem, msg);
    AspectValidationException exception3 = AspectValidationException.forPrecondition(testItem, msg);

    // Test equals
    assertEquals(exception1, exception2);
    assertNotEquals(exception1, exception3); // Different subType
    assertNotEquals(exception1, null);
    assertNotEquals(exception1, new Object());
    assertEquals(exception1, exception1); // Same instance

    // Test hashCode
    assertEquals(exception1.hashCode(), exception2.hashCode());
    assertNotEquals(exception1.hashCode(), exception3.hashCode());
  }

  @Test
  public void testToString() {
    AspectValidationException exception =
        AspectValidationException.forItem(testItem, "Test message");
    String str = exception.toString();

    assertNotNull(str);
    assertTrue(str.contains("AspectValidationException"));
    assertTrue(str.contains("item="));
    assertTrue(str.contains("changeType="));
    assertTrue(str.contains("entityUrn="));
    assertTrue(str.contains("aspectName="));
    assertTrue(str.contains("subType="));
    assertTrue(str.contains("msg="));
  }

  @Test
  public void testDifferentChangeTypes() throws URISyntaxException {
    // Test with different change types
    for (ChangeType changeType :
        new ChangeType[] {
          ChangeType.CREATE, ChangeType.DELETE, ChangeType.PATCH, ChangeType.UPSERT
        }) {
      BatchItem item = createTestBatchItem(changeType);

      AspectValidationException exception = AspectValidationException.forItem(item, "Test");
      assertEquals(exception.getChangeType(), changeType);
    }
  }

  @Test
  public void testNullMessage() {
    // Test that null message is handled properly
    AspectValidationException exception =
        new AspectValidationException(testItem, null, ValidationSubType.VALIDATION);

    assertNull(exception.getMsg());
    assertNull(exception.getMessage());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testNullBatchItem() {
    // Should throw NPE when item is null (as per @Nonnull annotation)
    AspectValidationException.forItem(null, "Error");
  }

  @Test
  public void testAllValidationSubTypes() {
    // Ensure all subtypes work correctly
    ValidationSubType[] subTypes = {
      ValidationSubType.VALIDATION,
      ValidationSubType.PRECONDITION,
      ValidationSubType.FILTER,
      ValidationSubType.AUTHORIZATION
    };

    for (ValidationSubType subType : subTypes) {
      AspectValidationException exception =
          new AspectValidationException(testItem, "Test", subType);
      assertEquals(exception.getSubType(), subType);
    }
  }

  @Test
  public void testExceptionChaining() {
    // Test exception chaining works properly
    RuntimeException rootCause = new RuntimeException("Root cause");
    IllegalArgumentException middleCause = new IllegalArgumentException("Middle cause", rootCause);
    AspectValidationException topException =
        AspectValidationException.forItem(testItem, "Top level", middleCause);

    assertEquals(topException.getCause(), middleCause);
    assertEquals(topException.getCause().getCause(), rootCause);
  }
}
