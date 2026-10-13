package com.linkedin.metadata.aspect.patch.template;

import static com.linkedin.metadata.Constants.ASSERTION_RUN_SUMMARY_ASPECT_NAME;

import com.linkedin.assertion.AssertionRunSummary;
import com.linkedin.assertion.AssertionStatus;
import com.linkedin.common.Status;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.metadata.aspect.patch.template.assertion.AssertionRunSummaryTemplate;
import com.linkedin.metadata.models.registry.SnapshotEntityRegistry;
import jakarta.json.Json;
import jakarta.json.JsonPatch;
import java.io.StringReader;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AssertionRunSummaryTemplateTest {

  private final AssertionRunSummaryTemplate template = new AssertionRunSummaryTemplate();

  @Test
  public void testGetDefaultReturnsEmptySummary() {
    AssertionRunSummary defaultSummary = template.getDefault();

    Assert.assertNotNull(defaultSummary);
    Assert.assertFalse(defaultSummary.hasLastPassedAtMillis());
    Assert.assertFalse(defaultSummary.hasLastFailedAtMillis());
    Assert.assertFalse(defaultSummary.hasLastErroredAtMillis());
    Assert.assertFalse(defaultSummary.hasLastInitializedAtMillis());
    Assert.assertFalse(defaultSummary.hasAssertionStatus());
  }

  @Test
  public void testGetTemplateType() {
    Assert.assertEquals(template.getTemplateType(), AssertionRunSummary.class);
  }

  @Test
  public void testTemplateIsRegisteredInSnapshotEntityRegistry() {
    SnapshotEntityRegistry registry = new SnapshotEntityRegistry();

    RecordTemplate defaultTemplate =
        registry.getAspectTemplateEngine().getDefaultTemplate(ASSERTION_RUN_SUMMARY_ASPECT_NAME);

    Assert.assertNotNull(defaultTemplate);
    Assert.assertTrue(defaultTemplate instanceof AssertionRunSummary);
  }

  @Test
  public void testGetSubtypeRejectsForeignRecordTemplate() {
    Assert.assertThrows(ClassCastException.class, () -> template.getSubtype(new Status()));
  }

  @Test
  public void testApplyPatchOntoDefaultSetsFields() throws Exception {
    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/lastPassedAtMillis\",\"value\":1700000000000},"
                            + "{\"op\":\"add\",\"path\":\"/assertionStatus\",\"value\":\"PASSING\"}]"))
                .readArray());

    AssertionRunSummary result = template.applyPatch(template.getDefault(), patch);

    Assert.assertEquals(result.getLastPassedAtMillis(), Long.valueOf(1700000000000L));
    Assert.assertEquals(result.getAssertionStatus(), AssertionStatus.PASSING);
  }

  @Test
  public void testApplySecondPatchPreservesEarlierFields() throws Exception {
    AssertionRunSummary existing = new AssertionRunSummary();
    existing.setLastPassedAtMillis(1700000000000L);
    existing.setAssertionStatus(AssertionStatus.PASSING);

    JsonPatch patch =
        Json.createPatch(
            Json.createReader(
                    new StringReader(
                        "[{\"op\":\"add\",\"path\":\"/lastFailedAtMillis\",\"value\":1600000000000}]"))
                .readArray());

    AssertionRunSummary result = template.applyPatch(existing, patch);

    Assert.assertEquals(result.getLastFailedAtMillis(), Long.valueOf(1600000000000L));
    // earlier fields must survive the partial update
    Assert.assertEquals(result.getLastPassedAtMillis(), Long.valueOf(1700000000000L));
    Assert.assertEquals(result.getAssertionStatus(), AssertionStatus.PASSING);
  }
}
