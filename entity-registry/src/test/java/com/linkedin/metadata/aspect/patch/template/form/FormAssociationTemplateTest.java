package com.linkedin.metadata.aspect.patch.template.form;

import static org.testng.Assert.*;

import com.linkedin.common.FormAssociation;
import com.linkedin.common.FormPromptAssociation;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import jakarta.json.Json;
import jakarta.json.JsonObjectBuilder;
import jakarta.json.JsonPatchBuilder;
import org.testng.annotations.Test;

public class FormAssociationTemplateTest {

  @Test
  public void testPatchPrompts() throws Exception {
    FormAssociationTemplate template = new FormAssociationTemplate();
    FormAssociation formAssociation = template.getDefault();
    JsonPatchBuilder patchBuilder = Json.createPatchBuilder();

    // Create a complex prompt with nested fields
    String promptId1 = "prompt-1";
    Urn actor = UrnUtils.getUrn("urn:li:corpuser:testUser");

    // Create field associations
    JsonObjectBuilder fieldPromptBuilder =
        Json.createObjectBuilder()
            .add("fieldPath", "testField")
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 123456789L).add("actor", actor.toString()));

    JsonObjectBuilder fieldAssociationsBuilder =
        Json.createObjectBuilder()
            .add("completedFieldPrompts", Json.createArrayBuilder().add(fieldPromptBuilder))
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 987654321L).add("actor", actor.toString()));

    // Create structured property response
    JsonObjectBuilder structuredPropertyBuilder =
        Json.createObjectBuilder()
            .add("propertyUrn", "urn:li:property:test")
            .add(
                "values",
                Json.createArrayBuilder()
                    .add(Json.createObjectBuilder().add("string", "test value")));

    JsonObjectBuilder responseBuilder =
        Json.createObjectBuilder().add("structuredPropertyResponse", structuredPropertyBuilder);

    // Add an incomplete prompt with all fields
    JsonObjectBuilder promptBuilder =
        Json.createObjectBuilder()
            .add("id", promptId1)
            .add("isComplete", false)
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 123456789L).add("actor", actor.toString()))
            .add("fieldAssociations", fieldAssociationsBuilder)
            .add("response", responseBuilder);

    patchBuilder.add("/prompts/" + promptId1, promptBuilder.build());

    FormAssociation result = template.applyPatch(formAssociation, patchBuilder.build());

    // Verify full prompt structure
    assertEquals(result.getIncompletePrompts().size(), 1);
    FormPromptAssociation addedPrompt = result.getIncompletePrompts().get(0);
    assertEquals(addedPrompt.getId(), promptId1);
    assertEquals(addedPrompt.getLastModified().getTime(), 123456789L);
    assertEquals(addedPrompt.getLastModified().getActor(), actor);
    assertNotNull(addedPrompt.getFieldAssociations());
    assertNotNull(addedPrompt.getResponse());
    assertEquals(
        addedPrompt.getFieldAssociations().getCompletedFieldPrompts().get(0).getFieldPath(),
        "testField");
    assertEquals(
        addedPrompt
            .getFieldAssociations()
            .getCompletedFieldPrompts()
            .get(0)
            .getLastModified()
            .getTime(),
        123456789L);

    // Move to completed and update some fields
    JsonPatchBuilder completePatchBuilder = Json.createPatchBuilder();

    // Add new field prompt
    JsonObjectBuilder newFieldPromptBuilder =
        Json.createObjectBuilder()
            .add("fieldPath", "newTestField")
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 987654321L).add("actor", actor.toString()));
    fieldPromptBuilder =
        Json.createObjectBuilder()
            .add("fieldPath", "testField")
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 123456789L).add("actor", actor.toString()));

    JsonObjectBuilder updatedFieldAssociationsBuilder =
        Json.createObjectBuilder()
            .add(
                "completedFieldPrompts",
                Json.createArrayBuilder().add(fieldPromptBuilder).add(newFieldPromptBuilder))
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 987654321L).add("actor", actor.toString()));

    // Add documentation response
    JsonObjectBuilder docResponseBuilder =
        Json.createObjectBuilder()
            .add(
                "documentationResponse",
                Json.createObjectBuilder().add("documentation", "Updated documentation"));

    JsonObjectBuilder completePromptBuilder =
        Json.createObjectBuilder()
            .add("id", promptId1)
            .add("isComplete", true)
            .add(
                "lastModified",
                Json.createObjectBuilder().add("time", 987654321L).add("actor", actor.toString()))
            .add("fieldAssociations", updatedFieldAssociationsBuilder)
            .add("response", docResponseBuilder);

    completePatchBuilder.add("/prompts/" + promptId1, completePromptBuilder.build());

    FormAssociation completeResult = template.applyPatch(result, completePatchBuilder.build());
    assertEquals(completeResult.getCompletedPrompts().size(), 1);
    assertEquals(completeResult.getIncompletePrompts().size(), 0);

    FormPromptAssociation completedPrompt = completeResult.getCompletedPrompts().get(0);
    assertEquals(completedPrompt.getLastModified().getTime(), 987654321L);
    assertEquals(completedPrompt.getLastModified().getActor(), actor);
    assertEquals(completedPrompt.getFieldAssociations().getCompletedFieldPrompts().size(), 2);
    assertEquals(
        completedPrompt
            .getFieldAssociations()
            .getCompletedFieldPrompts()
            .get(0)
            .getLastModified()
            .getTime(),
        123456789L);
    assertEquals(
        completedPrompt
            .getFieldAssociations()
            .getCompletedFieldPrompts()
            .get(1)
            .getLastModified()
            .getTime(),
        987654321L);
    assertNotNull(completedPrompt.getResponse().getDocumentationResponse());
    assertEquals(
        completedPrompt.getResponse().getDocumentationResponse().getDocumentation(),
        "Updated documentation");

    // Clean up
    JsonPatchBuilder cleanupBuilder = Json.createPatchBuilder();
    cleanupBuilder.remove("/prompts/" + promptId1);

    FormAssociation cleanResult = template.applyPatch(completeResult, cleanupBuilder.build());
    assertEquals(cleanResult, template.getDefault());
  }
}
