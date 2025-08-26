package com.linkedin.metadata.aspect.patch.template.form;

import static org.testng.Assert.*;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.FormVerificationAssociationArray;
import com.linkedin.common.Forms;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.aspect.patch.builder.FormsPatchBuilder;
import com.linkedin.mxe.MetadataChangeProposal;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatch;
import jakarta.json.JsonPatchBuilder;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import org.testng.annotations.Test;

public class FormsTemplateTest {

  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";

  @Test
  public void testPatchForms() throws Exception {
    FormsTemplate template = new FormsTemplate();
    Forms forms = template.getDefault();
    JsonPatchBuilder patchBuilder = Json.createPatchBuilder();

    // Create an incomplete form association
    Urn formUrn = UrnUtils.getUrn("urn:li:form:testForm1");
    String promptId1 = "prompt-1";
    Urn actor = UrnUtils.getUrn("urn:li:corpuser:testUser");

    // Create initial prompt
    JsonObjectBuilder promptBuilder =
        Json.createObjectBuilder()
            .add("id", promptId1)
            .add("isComplete", false)
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 123456789L).add("actor", actor.toString()));

    // Create form with prompts
    JsonObjectBuilder formBuilder =
        Json.createObjectBuilder()
            .add("urn", formUrn.toString())
            .add("isComplete", false)
            .add("prompts", Json.createObjectBuilder().add(promptId1, promptBuilder));

    patchBuilder.add("/forms/" + formUrn, formBuilder.build());

    Forms result = template.applyPatch(forms, patchBuilder.build());

    // Verify initial form state
    assertEquals(result.getIncompleteForms().size(), 1);
    FormAssociation addedForm = result.getIncompleteForms().get(0);
    assertEquals(addedForm.getUrn(), formUrn);
    assertEquals(addedForm.getIncompletePrompts().size(), 1);
    assertEquals(addedForm.getIncompletePrompts().get(0).getId(), promptId1);

    // Move form to completed and add a prompt
    JsonPatchBuilder completePatchBuilder = Json.createPatchBuilder();

    // Add another prompt
    String promptId2 = "prompt-2";
    JsonObjectBuilder newPromptBuilder =
        Json.createObjectBuilder()
            .add("id", promptId2)
            .add("isComplete", true)
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 987654321L).add("actor", actor.toString()));

    // First prompt now complete
    JsonObjectBuilder updatedPromptBuilder =
        Json.createObjectBuilder()
            .add("id", promptId1)
            .add("isComplete", true)
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 987654321L).add("actor", actor.toString()));

    JsonObjectBuilder completeFormBuilder =
        Json.createObjectBuilder()
            .add("urn", formUrn.toString())
            .add("isComplete", true)
            .add(
                "prompts",
                Json.createObjectBuilder()
                    .add(promptId1, updatedPromptBuilder)
                    .add(promptId2, newPromptBuilder));

    completePatchBuilder.add("/forms/" + formUrn, completeFormBuilder.build());

    Forms completeResult = template.applyPatch(result, completePatchBuilder.build());
    assertEquals(completeResult.getIncompleteForms().size(), 0);
    assertEquals(completeResult.getCompletedForms().size(), 1);

    FormAssociation completedForm = completeResult.getCompletedForms().get(0);
    assertEquals(completedForm.getUrn(), formUrn);
    assertEquals(completedForm.getCompletedPrompts().size(), 2);

    // Add verification
    JsonPatchBuilder verifyPatchBuilder = Json.createPatchBuilder();
    JsonObjectBuilder verificationBuilder =
        Json.createObjectBuilder().add("form", formUrn.toString());

    verifyPatchBuilder.add("/verifications/" + formUrn, verificationBuilder.build());

    Forms verifiedResult = template.applyPatch(completeResult, verifyPatchBuilder.build());
    assertEquals(verifiedResult.getVerifications().size(), 1);
    assertEquals(verifiedResult.getVerifications().get(0).getForm(), formUrn);

    // Clean up
    JsonPatchBuilder cleanupBuilder = Json.createPatchBuilder();
    cleanupBuilder.remove("/forms/" + formUrn);
    cleanupBuilder.remove("/verifications/" + formUrn);

    Forms cleanResult = template.applyPatch(verifiedResult, cleanupBuilder.build());
    assertEquals(
        cleanResult,
        template.getDefault().setVerifications(new FormVerificationAssociationArray()));
  }

  @Test
  public void testPatchWithBuilder() throws Exception {
    FormsTemplate template = new FormsTemplate();
    Forms forms = template.getDefault();

    Urn datasetUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    Urn formUrn = UrnUtils.getUrn("urn:li:form:testForm1");
    Urn actor = UrnUtils.getUrn("urn:li:corpuser:testUser");

    AuditStamp auditStamp = new AuditStamp().setActor(actor).setTime(123L);

    // Create a form with a prompt using the builder
    FormPromptAssociation prompt = new FormPromptAssociation();
    prompt.setId("test-prompt-1");
    prompt.setLastModified(auditStamp);

    FormsPatchBuilder builder = new FormsPatchBuilder();
    builder.urn(datasetUrn);
    FormAssociation formAssociation =
        new FormAssociation().setUrn(formUrn).setLastModified(auditStamp);
    builder.setFormIncomplete(formAssociation);
    builder.markPromptIncomplete(formUrn, prompt);
    JsonPatch patch = convertToJsonPatch(builder.build());

    Forms result = template.applyPatch(forms, patch);
    assertEquals(result.getIncompleteForms().size(), 1);
    FormAssociation addedForm = result.getIncompleteForms().get(0);
    assertEquals(addedForm.getIncompletePrompts().size(), 1);
    assertEquals(addedForm.getIncompletePrompts().get(0).getId(), prompt.getId());

    // Complete the prompt
    FormsPatchBuilder completeBuilder = new FormsPatchBuilder();
    completeBuilder.urn(datasetUrn);
    completeBuilder.completePrompt(formUrn, prompt);
    JsonPatch completePatch = convertToJsonPatch(completeBuilder.build());

    Forms completeResult = template.applyPatch(result, completePatch);
    assertEquals(completeResult.getIncompleteForms().size(), 1);
    assertEquals(completeResult.getIncompleteForms().get(0).getCompletedPrompts().size(), 1);
    assertEquals(completeResult.getIncompleteForms().get(0).getIncompletePrompts().size(), 0);

    // Complete the form
    FormAssociation form = completeResult.getIncompleteForms().get(0);
    FormsPatchBuilder formCompleteBuilder = new FormsPatchBuilder();
    formCompleteBuilder.urn(datasetUrn);
    formCompleteBuilder.completeForm(form);
    JsonPatch formCompletePatch = convertToJsonPatch(formCompleteBuilder.build());

    Forms formCompleteResult = template.applyPatch(completeResult, formCompletePatch);
    assertEquals(formCompleteResult.getIncompleteForms().size(), 0);
    assertEquals(formCompleteResult.getCompletedForms().size(), 1);

    // Add verification
    FormVerificationAssociation verification = new FormVerificationAssociation();
    verification.setForm(formUrn);
    FormsPatchBuilder verifyBuilder = new FormsPatchBuilder();
    verifyBuilder.urn(datasetUrn);
    verifyBuilder.verifyForm(verification);
    JsonPatch verifyPatch = convertToJsonPatch(verifyBuilder.build());

    Forms verifiedResult = template.applyPatch(formCompleteResult, verifyPatch);
    assertEquals(verifiedResult.getVerifications().size(), 1);
    assertEquals(verifiedResult.getVerifications().get(0).getForm(), formUrn);
  }

  @Test
  public void testBuildAndCleanup() throws Exception {
    FormsTemplate template = new FormsTemplate();
    Forms forms = template.getDefault();

    Urn datasetUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    Urn formUrn = UrnUtils.getUrn("urn:li:form:testForm1");
    Urn actor = UrnUtils.getUrn("urn:li:corpuser:testUser");

    FormAssociation form = new FormAssociation();
    form.setUrn(formUrn);

    // Add form with prompts and verify it
    FormPromptAssociation prompt = new FormPromptAssociation();
    prompt.setId("test-prompt-1");
    prompt.setLastModified(new AuditStamp().setActor(actor).setTime(123L));

    FormsPatchBuilder builder = new FormsPatchBuilder();
    builder.urn(datasetUrn);
    builder.completePrompt(formUrn, prompt);
    builder.completeForm(form);

    FormVerificationAssociation verification = new FormVerificationAssociation();
    verification.setForm(formUrn);
    builder.verifyForm(verification);

    JsonPatch patch = convertToJsonPatch(builder.build());
    Forms result = template.applyPatch(forms, patch);

    // Verify state before cleanup
    assertEquals(result.getCompletedForms().size(), 1);
    assertEquals(result.getVerifications().size(), 1);

    // Clean up everything
    FormsPatchBuilder cleanupBuilder = new FormsPatchBuilder();
    cleanupBuilder.urn(datasetUrn);
    cleanupBuilder.removeForm(formUrn);
    cleanupBuilder.removeFormVerification(formUrn);
    JsonPatch cleanupPatch = convertToJsonPatch(cleanupBuilder.build());

    Forms cleanResult = template.applyPatch(result, cleanupPatch);
    assertEquals(
        cleanResult,
        template.getDefault().setVerifications(new FormVerificationAssociationArray()));
  }

  public static JsonPatch convertToJsonPatch(MetadataChangeProposal mcp) {
    try {
      return Json.createPatch(
          Json.createReader(
                  new StringReader(mcp.getAspect().getValue().asString(StandardCharsets.UTF_8)))
              .readArray());
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Invalid JSON Patch: " + mcp.getAspect().getValue(), e);
    }
  }
}
