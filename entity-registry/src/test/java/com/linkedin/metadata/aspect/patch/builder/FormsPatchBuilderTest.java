package com.linkedin.metadata.aspect.patch.builder;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.FormVerificationAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import java.util.List;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class FormsPatchBuilderTest {

  private FormsPatchBuilder builder;
  private static final String TEST_ENTITY_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,SampleTable,PROD)";
  private static final String TEST_FORM_URN = "urn:li:form:testForm1";

  @BeforeMethod
  public void setup() throws Exception {
    builder = new FormsPatchBuilder();
    builder.urn(Urn.createFromString(TEST_ENTITY_URN));
  }

  @Test
  public void testCompletePrompt() throws Exception {
    Urn formUrn = UrnUtils.getUrn(TEST_FORM_URN);
    Urn actor = UrnUtils.getUrn("urn:li:corpuser:testUser");

    FormPromptAssociation prompt = new FormPromptAssociation();
    prompt.setId("test-prompt-1");
    prompt.setLastModified(new AuditStamp().setActor(actor).setTime(123L));

    builder.completePrompt(formUrn, prompt);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/forms/" + formUrn + "/prompts/" + prompt.getId());
    assertTrue(operation.getRight().has("isComplete"));
    assertTrue(operation.getRight().get("isComplete").asBoolean());
  }

  @Test
  public void testCompleteForm() throws Exception {
    Urn formUrn = UrnUtils.getUrn(TEST_FORM_URN);
    FormAssociation form = new FormAssociation();
    form.setUrn(formUrn);

    builder.completeForm(form);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/forms/" + formUrn);
    assertTrue(operation.getRight().has("isComplete"));
    assertTrue(operation.getRight().get("isComplete").asBoolean());
  }

  @Test
  public void testVerifyForm() throws Exception {
    Urn formUrn = UrnUtils.getUrn(TEST_FORM_URN);
    FormVerificationAssociation verification = new FormVerificationAssociation();
    verification.setForm(formUrn);

    builder.verifyForm(verification);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "add");
    assertEquals(operation.getMiddle(), "/verifications/" + formUrn);
    assertEquals(operation.getRight().get("form").asText(), formUrn.toString());
  }

  @Test
  public void testRemoveForm() throws Exception {
    Urn formUrn = UrnUtils.getUrn(TEST_FORM_URN);
    builder.removeForm(formUrn);
    builder.build();

    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 1);

    ImmutableTriple<String, String, JsonNode> operation = pathValues.get(0);
    assertEquals(operation.getLeft(), "remove");
    assertEquals(operation.getMiddle(), "/forms/" + formUrn);
    assertNull(operation.getRight());
  }

  @Test
  public void testBuildDoesNotAffectPathValues() throws Exception {
    Urn formUrn = UrnUtils.getUrn(TEST_FORM_URN);
    FormAssociation form = new FormAssociation();
    form.setUrn(formUrn);

    builder.completeForm(form).removeForm(formUrn);

    // First call build()
    builder.build();

    // Verify we can still access pathValues and they're correct
    List<ImmutableTriple<String, String, JsonNode>> pathValues = builder.getPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2);

    // Verify we can call build() again without issues
    builder.build();

    // And verify pathValues are still accessible and correct
    pathValues = builder.getPathValues();
    assertNotNull(pathValues);
    assertEquals(pathValues.size(), 2);
  }
}
